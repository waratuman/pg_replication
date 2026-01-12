# frozen_string_literal: true

require "pg"
require "pg/replication/version"

class PG::Replicator
  class Error < StandardError; end

  EPOCH = Time.new(2000, 1, 1, 0, 0, 0, 0)
  EPOCH_IN_MICROSECONDS = EPOCH.to_i * 1_000_000

  # To inspect an LSN @lsn.to_s(16).upcase.rjust(10, '0').insert(2, "/")
  MSG_KEEPALIVE = 'k'.ord  # 107
  MSG_WAL_DATA  = 'w'.ord  # 119

  attr_reader :host,
    :port,
    :dbname,
    :slot,
    :systemid,
    :start_position,
    :end_position,
    :timeline,
    :status_interval,
    :options,
    :last_server_lsn,
    :last_received_lsn,
    :last_processed_lsn,
    :last_message_send_time,
    :last_status

  # Note that the startpos option starts with the transaction that contains the
  # LSN, so if a transaction starts at LSN 1 and a startpos of 3 is specified
  # Postgres will start replication at LSN 1, even though 3 was specified.
  def initialize(*args, &block)
    @options = extract_replication_options(args)
    @connection_params = build_connection_params(args)
    extract_connection_params
    extract_replication_params

    @status_interval ||= connection.exec(<<~SQL).getvalue(0,0).to_i
      SELECT setting :: int FROM pg_catalog.pg_settings WHERE name = 'wal_receiver_status_interval';
    SQL

    @last_server_lsn = 0
    @last_received_lsn = 0
    @last_processed_lsn = 0
    @last_status = Time.now

    replicate(&block) if block
  end

  def connection
    return @connection if !@connection.nil?

    # Establish Connection
    @connection = PG.connect(@connection_params)

    if @connection.conninfo_hash[:replication] != 'database'
      raise Error, "Could not establish database-specific replication connection"
    end

    if !@connection
      raise Error, "Unable to create a connection"
    elsif @connection.status == PG::CONNECTION_BAD
      raise Error, "Connection failed: %s" % [ @connection.error_message ]
    end

    ##
    # This SQL statement installs an always-secure search path, so malicious
    # users can't take control.  CREATE of an unqualified name will fail, because
    # this selects no creation schema.  This does not demote pg_temp, so it is
    # suitable where we control the entire FE/BE connection but not suitable in
    # SECURITY DEFINER functions.  This is portable to PostgreSQL 7.3, which
    # introduced schemas.  When connected to an older version from code that
    # might work with the old server, skip this.
    if @connection.server_version >= 100000
      result = @connection.exec("SELECT pg_catalog.set_config('search_path', '', false);")

      if result.result_status != PG::PGRES_TUPLES_OK
        raise Error, "could not clear search_path: %s" % [ @connection.error_message ]
      end
    end

    ##
    # Ensure we have the same value of integer_datetimes (now always "on") as
    # the server we are connecting to.
    tmpparam = @connection.parameter_status("integer_datetimes")
    if !tmpparam
      raise Error, "could not determine server setting for integer_datetimes"
    end

    if tmpparam != "on"
      raise Error, "integer_datetimes compile flag does not match server"
    end

    @connection
  rescue => e
    @connection = nil
    raise e
  end

  def initialize_replication
    #= Replication setup.
    ident = connection.exec('IDENTIFY_SYSTEM;')[0]

    verify_systemid(ident)
    verify_timeline(ident)
    verify_dbname(ident)

    query = [ "START_REPLICATION SLOT" ]
    query << PG::Connection.escape_string(slot)
    query << "LOGICAL"
    query << @start_position.to_s(16).upcase.rjust(10, '0').insert(2, "/")

    query_options = []
    @options.each do |k, v|
      query_options << (PG::Connection.quote_ident(k) + " '" + PG::Connection.escape_string(v) + "'")
    end
    if !query_options.empty?
      query << '('
      query << query_options.join(', ')
      query << ')'
    end

    result = connection.exec(query.join(" "))

    if result.result_status != PG::PGRES_COPY_BOTH
      raise Error, "Could not send replication command \"%s\"" % [ query ]
    end

    result
  end

  def replicate(&block)
    initialize_replication

    loop do
      send_feedback(&block) if Time.now - last_status > status_interval

      break if @end_position != 0 && @last_processed_lsn >= @end_position

      connection.consume_input

      next if connection.is_busy

      begin
        result = connection.get_copy_data(async: true)#, decoder: TYPE_MAP)
      rescue PG::Error => e
        if e.message == "no COPY in progress\n"
          next
        else
          raise
        end
      end

      if result.nil? # Copy is done
        # Read the final result, which should be CommandComplete
        connection.get_last_result # The current implementation raises an error, so we don't have to?
        # result = connection.get_result
        # if result.result_status != PG::PGRES_COMMAND_OK
        #   raise result.error_message
        # end
        break
      elsif result === false
        select([connection.socket_io], nil, nil, status_interval)
      end

      next if result == false # No data yet

      case identifier = byte(result)
      when MSG_KEEPALIVE
        server_lsn = int64(result)
        send_time_us = int64(result) + EPOCH_IN_MICROSECONDS
        reply_requested = byte(result)

        @last_server_lsn = server_lsn if server_lsn != 0
        @last_message_send_time = Time.at(send_time_us / 1_000_000, send_time_us % 1_000_000, :microsecond)

        send_feedback(&block) if reply_requested == 1

        # If we've caught up to or past the end position, break
        break if @end_position != 0 && @last_server_lsn >= @end_position
      when MSG_WAL_DATA
        wal_start = int64(result)
        server_lsn = int64(result)
        send_time_us = int64(result) + EPOCH_IN_MICROSECONDS

        @last_received_lsn = wal_start if wal_start != 0
        @last_server_lsn = server_lsn if server_lsn != 0
        @last_message_send_time = Time.at(send_time_us / 1_000_000, send_time_us % 1_000_000, :microsecond)

        break if @end_position != 0 && @last_received_lsn > @end_position

        payload = result.force_encoding(connection.internal_encoding)
        yield payload
        @last_processed_lsn = @last_received_lsn
      else
        raise Error, "unrecognized streaming header: \"%c\"" % [ identifier ]
      end
    end

    send_feedback(&block)
  ensure
    connection.finish if connection
    @connection = nil
  end

  def close
    @connection&.close
    @connection = nil
  end

  private

  def extract_replication_options(args)
    return nil unless args[0].is_a?(Hash) && args[0][:replication_options]

    options = args[0][:replication_options].map do |k, v|
      PG::Connection.escape_string(k.to_s) <<
        "=" <<
        (PG::Connection.escape_string(case v
          when true then "on"
          when false then "off"
          else v.to_s
          end))
    end.join(' ')

    args[0].delete(:replication_options)
    args[0].delete_if { |k, v| v.nil? || v.to_s.empty? }

    options
  end

  def build_connection_params(args)
    params = PG::Connection.parse_connect_args(*args).dup

    if !(params =~ /replication='?database/)
      params << " replication='database'"
    end

    params
  end

  def extract_connection_params
    _, @host = *@connection_params.match(/host='([^\s]+)'/)
    @host = "localhost" if !@host
    @port = @connection_params.match(/port='([^\s]+)'/)&.[](1)&.to_i
    @dbname = @connection_params.match(/dbname='([^\s]+)'/)&.[](1)

    sub, @slot = *@connection_params.match(/slot='([^\s]+)'/)
    @connection_params.gsub!(sub, ' ')
  end

  def extract_replication_params
    sub, _, @start_position = *@connection_params.match(/(start_position|startpos)='([^\s]+)'/)
    if @start_position
      @connection_params.gsub!(sub, ' ')
      @start_position = parse_lsn(@start_position)
    else
      # Start replication at the last place left off according to the server.
      @start_position = 0
    end

    sub, _, @end_position = *@connection_params.match(/(end_position|endpos)='([^\s]+)'/)
    if @end_position
      @connection_params.gsub!(sub, ' ')
      @end_position = parse_lsn(@end_position)
    else
      @end_position = 0
    end

    sub, @timeline = *@connection_params.match(/timeline='([^\s]+)'/)
    if @timeline
      @connection_params.gsub!(sub, ' ')
      @timeline = @timeline.to_i
    end

    sub, @systemid = *@connection_params.match(/systemid='([^\s]+)'/)
    if @systemid
      @connection_params.gsub!(sub, ' ')
      @systemid = @systemid.to_i
    end

    sub, @status_interval = *@connection_params.match(/status_interval='([^\s]+)'/)

    if !@options
      sub, @options = *@connection_params.match(/replication_options='([^']+)'/)
      @connection_params.gsub!(sub, ' ') if @options
    end

    if @options
      @options = @options.split(/\s+/).inject({}) do |acc, x|
        k, v = x.split('=')
        acc[k] = v
        acc
      end
    else
      @options = {}
    end
  end

  def parse_lsn(value)
    case value
    when /\h{1,8}\/\h{1,8}/
      value.split("/").map { |s| s.rjust(8, '0') }.join.to_i(16)
    else
      Integer(value)
    end
  end

  def uint8(buffer)
    buffer.slice!(0).unpack('C')[0]
  end
  alias :byte :uint8

  def int64(buffer)
    a, b = buffer.slice!(0, 8).unpack('NN')
    (a << 32) + b
  end

  def verify_systemid(ident)
    if @systemid.nil?
      @systemid =  ident['systemid']
    elsif @systemid != ident['systemid']
      raise <<~MSG % [ @systemid, ident['systemid']]
        The systemid on server differs from the specified systemid.

          Specified systemid: %i
          Server systemid: %s
      MSG
    end
  end

  def verify_timeline(ident)
    if @timeline.nil?
      @timeline =  ident['timeline']
    elsif @timeline != ident['timeline']
      raise <<~MSG % [ @timeline, ident['timeline']]
        The timeline on server differs from the specified timeline.

          Specified timeline: %i
          Server timeline: %s
      MSG
    end
  end

  def verify_dbname(ident)
    if @dbname != ident['dbname']
      raise <<-MSG % [ @dbname, ident['dbname']]
        The database on server differs from the specified database.

        Specified database: %s
        Server database: %s
      MSG
    end
  end

  def send_feedback
    @last_status = Time.now
    timestamp = ((last_status - EPOCH) * 1000000).to_i

    lsn_location = if @last_processed_lsn == 0
      0
    else
      @last_processed_lsn + 1
    end

    msg = ('r'.codepoints + [
      lsn_location,
      lsn_location,
      lsn_location,
      timestamp,
      0
    ]).pack('CQ>Q>Q>Q>C')

    connection.put_copy_data(msg)
    connection.flush

    # Yield nil to notify the caller that a feedback message was sent.
    # This allows users to track when feedback occurs (e.g., for logging or
    # breaking out of the replication loop after catching up).
    yield(nil) if block_given?
  end

end
