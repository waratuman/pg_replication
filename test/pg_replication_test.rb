require "test_helper"

class PGReplicationTest < Minitest::Test

  def connection
    if @connection
      if @connection.finished?
        @connection = PG::Connection.new(dbname: "pg_replication_test")
      else
        @connection
      end
    else
      @connection = PG::Connection.new(dbname: "pg_replication_test")
    end
  end

  def setup
    system('createdb', 'pg_replication_test')
  end

  def test_replication
    dbname = connection.conninfo_hash[:dbname]
    host = connection.conninfo_hash[:host] || 'localhost'
    port = connection.conninfo_hash[:port]

    system('pg_recvlogical',
      '-h', host,
      '-p', port,
      '-d', dbname,
      '--slot', 'pg_replication_test_slot',
      '--create-slot',
      '-P', 'test_decoding')

    results = []
    t = Thread.new do
      PG::Replication.new(connection.conninfo_hash.merge({
        slot: 'pg_replication_test_slot',
        replication_options: { "include-timestamp" => true }
      }).select { |_, v| !v.nil? }) do |res|
        results << res
        Thread.exit if results.size >= 5
      end
    end

    # Wait for replication to start
    sleep 2

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

    system('pg_recvlogical',
      '-h', host || 'localhost',
      '-p', port,
      '-d', dbname,
      '--slot', 'pg_replication_test_slot',
      '--drop-slot',
      '-P', 'test_decoding')
  end

end
