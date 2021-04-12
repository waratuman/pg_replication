$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "pg_replication"

require "minitest/autorun"

class Minitest::Test

  def lsn(value)
    case value
    when Integer
      [value >> 32, value & 0xFFFFFFFF].map { |x| x.to_s(16).upcase }.join("/")
    else
      value.to_s
    end
  end

end
