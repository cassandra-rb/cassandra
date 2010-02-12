require "#{File.dirname(__FILE__)}/test_helper"
require "thrift/server/mongrel_http_server"

class ThriftClientHTTPTest < Test::Unit::TestCase

  def setup
    @servers = ["http://127.0.0.1:1461/greeter", "http://127.0.0.1:1462/greeter", "http://127.0.0.1:1463/greeter"]
    @socket = 1461
    @timeout = 0.2
    @options = {:protocol_extra_params => [false]}
    @pid = Process.fork do
      Signal.trap("INT") { exit }
      Greeter::HTTPServer.new(@servers.last).serve
    end
    # Need to give the child process a moment to open the listening socket or
    # we get occasional "could not connect" errors in tests.
    sleep 0.05
  end

  def teardown
    Process.kill("INT", @pid)
    Process.wait
  end

  def test_bad_uri
    assert_raises URI::InvalidURIError do
      @options.merge!({ :protocol => Thrift::BinaryProtocol, :transport => Thrift::HTTPClientTransport })
      ThriftClient.new(Greeter::Client, "127.0.0.1:1463", @options).greeting("someone")
    end
  end

  def test_bad_uri_no_http
    assert_raises ArgumentError do
      @options.merge!({ :protocol => Thrift::BinaryProtocol, :transport => Thrift::HTTPClientTransport })
      ThriftClient.new(Greeter::Client, "//127.0.0.1:1463", @options).greeting("someone")
    end
  end

  def test_valid_server
    assert_nothing_raised do
      @options.merge!({ :protocol => Thrift::BinaryProtocol, :transport => Thrift::HTTPClientTransport })
      ThriftClient.new(Greeter::Client, "http://127.0.0.1:1463/greeter", @options).greeting("someone")
    end
  end

end