require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase

  def setup
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
    @socket = 1461
    @timeout = 0.2
    @options = {:protocol_extra_params => [false]}
    @pid = Process.fork do
      Signal.trap("INT") { exit }
      Greeter::Server.new("1463").serve
    end
    # Need to give the child process a moment to open the listening socket or
    # we get occasional "could not connect" errors in tests.
    sleep 0.05
  end

  def teardown
    Process.kill("INT", @pid)
    Process.wait
  end

  def test_live_server
    assert_nothing_raised do
      ThriftClient.new(Greeter::Client, @servers.last, @options).greeting("someone")
    end
  end

  def test_non_random_fall_through
    assert_nothing_raised do
      ThriftClient.new(Greeter::Client, @servers, @options.merge(:randomize_server_list => false)).greeting("someone")
    end
  end

  def test_dont_raise
    assert_nothing_raised do
      ThriftClient.new(Greeter::Client, @servers.first, @options.merge(:raise => false)).greeting("someone")
    end
  end

  def test_dont_raise_with_defaults
    client = ThriftClient.new(Greeter::Client, @servers.first, @options.merge(:raise => false, :defaults => {:greeting => 1}))
    assert_equal 1, client.greeting
  end

  def test_defaults_dont_override_no_method_error
    client = ThriftClient.new(Greeter::Client, @servers, @options.merge(:raise => false, :defaults => {:Missing => 2}))
    assert_raises(NoMethodError) { client.Missing }
  end

  def test_random_fall_through
    assert_nothing_raised do
      10.times do
        client = ThriftClient.new(Greeter::Client, @servers, @options)
        client.greeting("someone")
        client.disconnect!
      end
    end
  end

  def test_lazy_connection
    assert_nothing_raised do
      ThriftClient.new(Greeter::Client, @servers[0,2])
    end
  end

  def test_no_servers_eventually_raise
    client = ThriftClient.new(Greeter::Client, @servers[0,2], @options)
    assert_raises(ThriftClient::NoServersAvailable) do
      client.greeting("someone")
      client.disconnect!
    end
  end

  def test_framed_transport_timeout
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(Greeter::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout)
          ).greeting("someone")
        end
      end
      assert((measurement.real > @timeout), "#{measurement.real} < #{@timeout}")
    end
  end

  def test_buffered_transport_timeout
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(Greeter::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout, :transport_wrapper => Thrift::BufferedTransport)
          ).greeting("someone")
        end
      end
      assert((measurement.real > @timeout), "#{measurement.real} < #{@timeout}")
    end
  end

  def test_buffered_transport_timeout_override
    # FIXME Large timeout values always are applied twice for some bizarre reason
    log_timeout = @timeout * 4
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(Greeter::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout, :timeout_overrides => {:greeting => log_timeout}, :transport_wrapper => Thrift::BufferedTransport)
          ).greeting("someone")
        end
      end
      assert((measurement.real > log_timeout), "#{measurement.real} < #{log_timeout}")
    end
  end

  def test_retry_period
    client = ThriftClient.new(Greeter::Client, @servers[0,2], @options.merge(:server_retry_period => 1))
    assert_raises(ThriftClient::NoServersAvailable) { client.greeting("someone") }
    sleep 1.1
    assert_raises(ThriftClient::NoServersAvailable) { client.greeting("someone") }
  end

  def test_server_max_requests
    client = ThriftClient.new(Greeter::Client, @servers, @options.merge(:server_max_requests => 2))
    client.greeting("someone")
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client
    client.greeting("someone") 
    assert_not_equal internal_client, client.client
  end

  private

  def stub_server(port)
    socket = TCPServer.new('127.0.0.1', port)
    Thread.new { socket.accept }
    yield socket
  ensure
    socket.close
  end
end