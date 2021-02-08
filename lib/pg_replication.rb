# frozen_string_literal: true

require "pg"
require "pg/replication/version"

class PG::Replicator
  class Error < StandardError; end

  EPOCH = Time.new(2000, 1, 1, 0, 0, 0)

  # To inspect an LSN @lsn.to_s(16).upcase.rjust(10, '0').insert(2, "/")
  attr_accessor :host,
    :port,
    :dbname,
    :slot,
    :systemid,
    :xlogpos,
    :timeline,
    :options,
    :last_received_lsn,
    :last_processed_lsn,
    :last_server_lsn,
    :last_status

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

    sub, @xlogpos = *@connection_params.match(/xlogpos='([^\s]+)'/)
    if @xlogpos
      @connection_params.gsub!(sub, ' ')
      @xlogpos = case @xlogpos
      when /\h{1,8}\/\h{1,8}/
        @xlogpos.sub("/", "").to_i(16)
      else
        Integer(@xlogpos)
      end
    else
      # Start replication at the last place left off according to the server.
      @xlogpos = 0 if !@xlogpos
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

    self.last_received_lsn = nil
    self.last_processed_lsn = nil
    self.last_status = Time.now

    replicate(&block) if block
  end

  def connection
    return @connection if instance_variable_defined?(:@connection)

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
    query << @xlogpos.to_s(16).upcase.rjust(10, '0').insert(2, "/")

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

  def replicate
    initialize_replication

    loop do
      send_feedback if Time.now - self.last_status > 10
      connection.consume_input

      next if connection.is_busy

      begin
        result = connection.get_copy_data(async: true)
      rescue PG::Error => e
        if e.message == "no COPY in progress\n"
          next
        else
          raise
        end
      end

      break if result.nil?    # Copy is done
      next if result == false # No data yet

      case result[0]
      when 'k' # Keepalive
        a1, a2 = result[1..8].unpack('NN')
        self.last_server_lsn = (a1 << 32) + a2
        send_feedback if result[9] == "\x01"
      when 'w' # WAL data
        a1, a2, b1, b2, c1, c2 = result[1..25].unpack('NNNNNN')
        self.last_received_lsn = (a1 << 32) + a2
        self.last_server_lsn = (b1 << 32) + b2
        timestamp = (c1 << 32) + c2
        data = result[25..-1].force_encoding(connection.internal_encoding)
        yield data
        self.last_processed_lsn = self.last_received_lsn
      else
        raise "unrecognized streaming header: \"%c\"" % [ result[0] ]
      end
    end
  ensure
    connection.finish if connection
  end

  def close
    connection.close
  end

  private

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
    self.last_status = Time.now
    timestamp = ((last_status - EPOCH) * 1000000).to_i
    msg = ('r'.codepoints + [
      self.last_received_lsn >> 32,
      self.last_received_lsn,
      self.last_received_lsn >> 32,
      self.last_received_lsn,
      self.last_processed_lsn >> 32,
      self.last_processed_lsn,
      timestamp >> 32,
      timestamp,
      0
    ]).pack('CNNNNNNNNC')

    connection.put_copy_data(msg)
    connection.flush
  end

end
