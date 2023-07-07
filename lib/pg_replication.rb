# frozen_string_literal: true

require "pg"
require "pg/replication/version"

class PG::Replicator
  class Error < StandardError; end

  EPOCH = Time.new(2000, 1, 1, 0, 0, 0, 0)
  EPOCH_IN_MICROSECONDS = EPOCH.to_i * 1_000_000

  # To inspect an LSN @lsn.to_s(16).upcase.rjust(10, '0').insert(2, "/")
  attr_accessor :host,
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
    case args[0]
    when Hash
      @options = if args[0][:replication_options]
        args[0][:replication_options].map do |k, v|
          PG::Connection.escape_string(k.to_s) <<
            "=" <<
            (PG::Connection.escape_string(case v
              when true then "on"
              when false then "off"
              else v.to_s
              end))
        end.join(' ')
      else
        String.new
      end
      args[0].delete(:replication_options)
      args[0].delete_if { |k, v| v.nil? || v.to_s.empty? }
    end

    @connection_params = PG::Connection.parse_connect_args(*args).dup

    if !(@connection_params =~ /replication='?database/)
      @connection_params << " replication='database'"
    end

    _, @host = *@connection_params.match(/host='([^\s]+)'/)
    @host = "localhost" if !@host
    @port = @connection_params.match(/port='([^\s]+)'/)[1]&.to_i
    @dbname = @connection_params.match(/dbname='([^\s]+)'/)[1]

    sub, @slot = *@connection_params.match(/slot='([^\s]+)'/)
    @connection_params.gsub!(sub, ' ')

    sub, _, @start_position = *@connection_params.match(/(start_position|startpos)='([^\s]+)'/)
    if @start_position
      @connection_params.gsub!(sub, ' ')
      @start_position = case @start_position
      when /\h{1,8}\/\h{1,8}/
        @start_position.split("/").map { |s| s.rjust(8, '0') }.join.to_i(16)
      else
        Integer(@start_position)
      end
    else
      # Start replication at the last place left off according to the server.
      @start_position = 0 if !@start_position
    end

    sub, _, @end_position = *@connection_params.match(/(end_position|endpos)='([^\s]+)'/)
    if @end_position
      @connection_params.gsub!(sub, ' ')
      @end_position = case @end_position
      when /\h{1,8}\/\h{1,8}/
        @end_position.split("/").map { |s| s.rjust(8, '0') }.join.to_i(16)
      else
        Integer(@end_position)
      end
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
      raise PG::Error.new("Could not establish database-specific replication connection");
    end

    if !@connection
      raise "Unable to create a connection"
    elsif @connection.status == PG::CONNECTION_BAD
      raise "Connection failed: %s" % [ @connection.error_message ]
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
        raise "could not clear search_path: %s" % [ @connection.error_message ]
      end
    end

    ##
    # Ensure we have the same value of integer_datetimes (now always "on") as
    # the server we are connecting to.
    tmpparam = @connection.parameter_status("integer_datetimes")
    if !tmpparam
      raise "could not determine server setting for integer_datetimes"
    end

    if tmpparam != "on"
      raise "integer_datetimes compile flag does not match server"
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
      raise PG::InvalidResultStatus.new("Could not send replication command \"%s\"" % [ query ])
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
      when 107 # Byte1('k') Keepalive
        a = int64(result)
        b = int64(result) + EPOCH_IN_MICROSECONDS
        c = byte(result)

        @last_server_lsn = a if a != 0
        @last_message_send_time = Time.at(b / 1_000_000, b % 1_000_000, :microsecond)

        send_feedback(&block) if c == 1
      when 119 # Byte1('w') WAL data
        a = int64(result)
        b = int64(result)
        c = int64(result) + EPOCH_IN_MICROSECONDS

        @last_received_lsn = a if a != 0
        @last_server_lsn = b if b != 0
        @last_message_send_time = Time.at(c / 1_000_000, c % 1_000_000, :microsecond)

        payload = result.force_encoding(connection.internal_encoding)
        yield payload
        @last_processed_lsn = @last_received_lsn
      else
        raise "unrecognized streaming header: \"%c\"" % [ identifier ]
      end
    end

    send_feedback(&block)
  ensure
    connection.finish if connection
    @connection = nil
  end

  def close
    connection.close
    @connection = nil
  end

  private

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
    msg = ('r'.codepoints + [
      @last_processed_lsn >> 32,
      @last_processed_lsn + 1,
      @last_processed_lsn >> 32,
      @last_processed_lsn + 1,
      @last_processed_lsn >> 32,
      @last_processed_lsn + 1,
      timestamp >> 32,
      timestamp,
      0
    ]).pack('CNNNNNNNNC')

    connection.put_copy_data(msg)
    connection.flush

    yield(nil) if block_given?
  end

end
