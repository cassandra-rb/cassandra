require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase
  def setup
    @entry = [ScribeThrift::LogEntry.new(:message => "something", :category => "thrift_client")]
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
    @socket = 1461
    @timeout = 0.2
    @options = {:protocol_extra_params => [false]}
  end

  def test_single_server_with_no_errors
    client = ThriftClient.new(ScribeThrift::Client, @servers.last, @options)
    client.expects(:transport_instance).with("127.0.0.1", "1463").returns(mock("Transport", :open))
    client.expects(:internal_client_instance).returns(mock("InternalClient", :Log))
    
    assert_nothing_raised do
      client.Log(@entry)
    end
  end

  def test_non_random_fall_through
    client = ThriftClient.new(ScribeThrift::Client, @servers, @options.merge(:randomize_server_list => false))
    transport_fails_connect = stub("Transport", :close)
    transport_fails_connect.expects(:open).times(2).raises(Thrift::TransportException)
    client.expects(:transport_instance).with("127.0.0.1", "1463").returns(transport_fails_connect)
    client.expects(:transport_instance).with("127.0.0.1", "1462").returns(transport_fails_connect)
    client.expects(:transport_instance).with("127.0.0.1", "1461").returns(mock("Transport", :open))
    client.expects(:internal_client_instance).returns(mock("InternalClient", :Log))
    
    assert_nothing_raised do
      client.Log(@entry)
    end
  end

  def test_dont_raise
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers.first, @options.merge(:raise => false)).Log(@entry)
    end
  end

  def test_dont_raise_with_defaults
    client = ThriftClient.new(ScribeThrift::Client, @servers.first, @options.merge(:raise => false, :defaults => {:Log => 1}))
    assert_equal 1, client.Log(@entry)
  end

  def test_defaults_dont_override_no_method_error
    client = ThriftClient.new(ScribeThrift::Client, @servers, @options.merge(:raise => false, :defaults => {:Missing => 2}))
    assert_raises(NoMethodError) { client.Missing(@entry) }
  end

  def test_random_fall_through
    client = ThriftClient.new(ScribeThrift::Client, @servers, @options)
    client.stubs(:transport_instance).returns(stub("Transport", :open))
    client.stubs(:internal_client_instance).returns(stub("InternalClient", :Log))
    
    assert_nothing_raised do
      10.times { client.Log(@entry) }
    end
  end

  def test_lazy_connection
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers[0,2])
    end
  end

  def test_no_servers_eventually_raise
    client = ThriftClient.new(ScribeThrift::Client, @servers[0,2], @options)
    assert_raises(ThriftClient::NoServersAvailable) do
      client.Log(@entry)
      client.disconnect!
    end
  end

  def test_framed_transport_timeout
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(ScribeThrift::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout)
          ).Log(@entry)
        end
      end
      assert((measurement.real > @timeout - 0.01), "#{measurement.real} < #{@timeout}")
      assert((measurement.real < @timeout + 0.01), "#{measurement.real} > #{@timeout}")
    end
  end

  def test_buffered_transport_timeout
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(ScribeThrift::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout, :transport => Thrift::BufferedTransport)
          ).Log(@entry)
        end
      end
      assert((measurement.real > @timeout - 0.01), "#{measurement.real} < #{@timeout}")
      assert((measurement.real < @timeout + 0.01), "#{measurement.real} > #{@timeout}")
    end
  end

  def test_buffered_transport_timeout_override
    # FIXME Large timeout values always are applied twice for some bizarre reason
    log_timeout = @timeout * 4
    stub_server(@socket) do |socket|
      measurement = Benchmark.measure do
        assert_raises(Thrift::TransportException) do
          ThriftClient.new(ScribeThrift::Client, "127.0.0.1:#{@socket}",
            @options.merge(:timeout => @timeout, :timeout_overrides => {:Log => log_timeout}, :transport => Thrift::BufferedTransport)
          ).Log(@entry)
        end
      end
      assert((measurement.real > log_timeout - 0.01), "#{measurement.real} < #{log_timeout }")
      assert((measurement.real < log_timeout + 0.01), "#{measurement.real} > #{log_timeout}")
    end
  end

  def test_retry_period
    client = ThriftClient.new(ScribeThrift::Client, @servers[0,2], @options.merge(:server_retry_period => 1))
    assert_raises(ThriftClient::NoServersAvailable) { client.Log(@entry) }
    sleep 1.1
    assert_raises(ThriftClient::NoServersAvailable) { client.Log(@entry) }
  end
  
  def test_server_max_requests
    client = ThriftClient.new(ScribeThrift::Client, @servers, @options.merge(:server_max_requests => 2))
    trans1 = mock("Transport1", :open, :close)
    trans2 = mock("Transport2", :open)
    client.expects(:transport_instance).times(2).returns(trans1).then.returns(trans2)
    internal_client_instance = mock("InternalClient")
    internal_client_instance.expects(:Log).times(2)
    client.expects(:internal_client_instance).times(2).returns(internal_client_instance).then.returns(mock("InternalClient2", :Log))
    
    client.Log(@entry)
    internal_client = client.client
    client.Log(@entry)
    assert_equal internal_client, client.client
    client.Log(@entry)  
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