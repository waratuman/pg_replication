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

    system('dropdb', '--if-exists', '--force', 'pg_replication_test')
  end

  def test_replication
    connection.exec(<<-SQL)
      CREATE TABLE teas ( kind TEXT );
      INSERT INTO teas VALUES ( '煎茶' )
          , ( '蕎麦茶' )
          , ( '魔茶' );
    SQL

    results = []
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    # LSN should be 0 before starting. 0 is an invalid LSN according to
    # https://github.com/postgres/postgres/blob/2dbe8905711ba09a2214b6e835f8f0c2c4981cb3/src/include/access/xlogdefs.h#L23-L28
    assert_equal 0, replicator.last_server_lsn
    assert_equal 0, replicator.last_received_lsn
    assert_equal 0, replicator.last_processed_lsn

    replicator.replicate do |res|
      results << res
      break if results.size >= 5
    end

    assert_match(/^BEGIN\s\d+$/, results[0])
    [ '煎茶', '蕎麦茶', '魔茶' ].each_with_index do |tea, i|
      assert_equal("table public.teas: INSERT: kind[text]:'#{tea}'",
        results[i + 1])
    end
    assert_match(/^COMMIT\s\d+\s\(at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?[-+]\d{2}\)$/,
      results[4])
  ensure
    if connection
      connection.exec("DROP TABLE IF EXISTS teas;")
    end
  end

  def test_replication_with_endpos
    connection.exec(<<-SQL)
      CREATE TABLE teas ( kind TEXT );
      INSERT INTO teas VALUES ( '煎茶' )
          , ( '蕎麦茶' )
          , ( '魔茶' );
    SQL

    lsn = connection.exec(<<~SQL)[0]['lsn']#.split("/").pack('H4H4').bytes.inject(0) { |acc, x| (acc << 8) + x }
      SELECT pg_current_wal_insert_lsn() AS lsn;
    SQL

    connection.exec(<<~SQL)
      INSERT INTO teas (kind) VALUES ( 'ハーブティー' );
    SQL

    results = []
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      endpos: lsn,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    # LSN should be 0 before starting. 0 is an invalid LSN according to
    # https://github.com/postgres/postgres/blob/2dbe8905711ba09a2214b6e835f8f0c2c4981cb3/src/include/access/xlogdefs.h#L23-L28
    assert_equal 0, replicator.last_server_lsn
    assert_equal 0, replicator.last_received_lsn
    assert_equal 0, replicator.last_processed_lsn

    replicator.replicate do |res|
      results << res
    end

    assert_match(/^BEGIN\s\d+$/, results[0])
    [ '煎茶', '蕎麦茶', '魔茶' ].each_with_index do |tea, i|
      assert_equal("table public.teas: INSERT: kind[text]:'#{tea}'",
        results[i + 1])
    end

    assert !results.any? { |x| x == "table public.teas: INSERT: kind[text]:'ハーブティー'" }

    assert_match(/^COMMIT\s\d+\s\(at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?[-+]\d{2}\)$/,
      results[4])
  ensure
    if connection
      connection.exec("DROP TABLE IF EXISTS teas;")
    end
  end

  def test_last_server_lsn
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    # LSN should be 0 before starting. 0 is an invalid LSN according to
    # https://github.com/postgres/postgres/blob/2dbe8905711ba09a2214b6e835f8f0c2c4981cb3/src/include/access/xlogdefs.h#L23-L28
    assert_equal 0, replicator.last_server_lsn
    assert_equal 0, replicator.last_received_lsn
    assert_equal 0, replicator.last_processed_lsn

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
    sleep(0.1) while !t.status.nil? && t.status && replicator.last_server_lsn == 0

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
      start_position: "3B/6C036B08",
      timeline: 2,
      status_interval: 10,
      replication_options: { "include-timestamp" => true }
    }

    replicator = PG::Replicator.new(options)
    assert_equal dbname, replicator.dbname
    assert_equal host, replicator.host
    assert_equal port, replicator.port
    assert_equal slot, replicator.slot
    assert_equal 255215233800, replicator.start_position
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

  def test_start_position
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      start_position: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      start_position: "0/0",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 0, replicator.start_position

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      start_position: "FFFFFFFF/FFFFFFFF",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 2 ** 64 - 1, replicator.start_position

    replicator.initialize_replication
    replicator.close
  end

  def test_startpos
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      startpos: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      startpos: "0/0",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 0, replicator.start_position

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      startpos: "FFFFFFFF/FFFFFFFF",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 2 ** 64 - 1, replicator.start_position

    replicator.initialize_replication
    replicator.close
  end

  def test_end_position
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      end_position: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      end_position: "0/0",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 0, replicator.end_position

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      end_position: "FFFFFFFF/FFFFFFFF",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 2 ** 64 - 1, replicator.end_position

    replicator.initialize_replication
    replicator.close
  end

  def test_endpos
    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      endpos: 2,
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      endpos: "0/0",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })
    assert_equal 0, replicator.end_position

    replicator.initialize_replication
    replicator.close

    replicator = PG::Replicator.new(connection.conninfo_hash.merge({
      slot: slot,
      endpos: "FFFFFFFF/FFFFFFFF",
      replication_options: { "include-timestamp" => true }
    }).select { |_, v| !v.nil? })

    replicator.initialize_replication
    replicator.close
  end
end
