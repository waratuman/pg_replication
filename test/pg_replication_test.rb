require "test_helper"

class PGReplicationTest < Minitest::Test

  attr_accessor :dbname, :host, :port, :slot

  def connection
    if instance_variable_defined?(:@connection)# || @connection
      if @connection.finished?
        @connection = PG::Connection.new(dbname: dbname)
      else
        @connection
      end
    else
      @connection = PG::Connection.new(dbname: "pg_replication_test")
    end
  end

  def setup
    @dbname = "pg_replication_test"
    @host = "localhost"
    @port = 5432
    @slot = "pg_replication_test_slot"

    system('createdb', 'pg_replication_test')
    system('pg_recvlogical',
      '-h', host,
      '-p', port.to_s,
      '-d', dbname,
      '--slot', slot,
      '--create-slot',
      '-P', 'test_decoding')
  end

  def teardown
    connection.close

    system('pg_recvlogical',
      '-h', host || 'localhost',
      '-p', port.to_s,
      '-d', dbname,
      '--slot', slot,
      '--drop-slot',
      '-P', 'test_decoding')

    system('dropdb', '--if-exists', 'pg_replication_test')
  end

  def test_replication
    results = []
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    # Should be nil before starting, 0 is a LSN
    assert_nil replicator.last_server_lsn
    assert_nil replicator.last_received_lsn
    assert_nil replicator.last_processed_lsn

    t = Thread.new do
      replicator.replicate do |res|
        results << res
        Thread.exit if results.size >= 5
      end
    end

    # Wait for replication to start
    sleep(0.1) while !t.status.nil? && t.status && replicator.last_server_lsn.nil?

    connection.exec(<<-SQL)
      CREATE TABLE teas ( kind TEXT );
      INSERT INTO teas VALUES ( '煎茶' )
          , ( '蕎麦茶' )
          , ( '魔茶' );
    SQL

    t.join

    assert_match(/^BEGIN\s\d+$/, results[0])
    [ '煎茶', '蕎麦茶', '魔茶' ].each_with_index do |tea, i|
      assert_equal("table public.teas: INSERT: kind[text]:'#{tea}'",
        results[i + 1])
    end
    assert_match(/^COMMIT\s\d+\s\(at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?[-+]\d{2}\)$/,
      results[4])
  ensure
    t.kill if t

    if connection
      connection.exec("DROP TABLE IF EXISTS teas;")
    end
  end

  def test_last_server_lsn
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    # Should be nil before starting, 0 is a LSN
    assert_nil replicator.last_server_lsn
    assert_nil replicator.last_received_lsn
    assert_nil replicator.last_processed_lsn

    pause_replication = false
    t = Thread.new do
      results = []
      replicator.replicate do |res|
        results << res
        sleep(0.1) while pause_replication
        Thread.exit if results.size >= 5
      end
    end

    # Wait for replication to start
    sleep(0.1) while !t.status.nil? && t.status && replicator.last_server_lsn.nil?

    pause_replication = true

    last_server_lsn = connection.exec("SELECT pg_current_wal_lsn();").getvalue(0,0)
    assert_equal last_server_lsn, lsn(replicator.last_server_lsn)
    assert_in_delta Time.now, replicator.last_message_send_time, 1

    connection.exec(<<-SQL)
      CREATE TABLE teas ( kind TEXT );
      INSERT INTO teas VALUES ( '煎茶' )
          , ( '蕎麦茶' )
          , ( '魔茶' );
    SQL

    last_server_lsn = connection.exec("SELECT pg_current_wal_lsn();").getvalue(0,0)
    pause_replication = false

    t.join
    assert_equal last_server_lsn, lsn(replicator.last_server_lsn)
    assert_in_delta Time.now, replicator.last_message_send_time, 1
  ensure
    t.kill if t

    if connection
      connection.exec("DROP TABLE IF EXISTS teas;")
    end
  end

  def test_initialize
    options = {
      dbname: dbname,
      host: host,
      port: port,
      slot: slot,
      xlogpos: "3B/6C036B08",
      timeline: 2,
      replication_options: { "include-timestamp" => true }
    }

    replicator = PG::Replicator.new(options)
    assert_equal dbname, replicator.dbname
    assert_equal host, replicator.host
    assert_equal port, replicator.port
    assert_equal slot, replicator.slot
    assert_equal 255215233800, replicator.xlogpos
    assert_equal 2, replicator.timeline
    assert_equal({ "include-timestamp" => "on" }, replicator.options)
  end

  def test_timeline
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      timeline: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    error = assert_raises RuntimeError do
      replicator.initialize_replication
    ensure
      replicator.close
    end

    assert_match(/The timeline on server differs from the specified timeline./, error.message)
    assert_match(/Specified timeline: 2/, error.message)
    assert_match(/Server timeline: 1/, error.message)
  end

  def test_systemid
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      systemid: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    error = assert_raises RuntimeError do
      replicator.initialize_replication
    ensure
      replicator.close
    end

    assert_match(/The systemid on server differs from the specified systemid./, error.message)
    assert_match(/Specified systemid: 2/, error.message)
    assert_match(/Server systemid: \d+/, error.message)
  end

  def test_xlogpos
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      xlogpos: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      xlogpos: "0/0",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      xlogpos: "FFFFFFFF/FFFFFFFF",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close
  end

end
