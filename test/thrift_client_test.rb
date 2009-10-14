require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase
  def test_thrift_client_success
    ThriftClient.new(ScribeThrift::Client, "localhost", @port)
  end
end
