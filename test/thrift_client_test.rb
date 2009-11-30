require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase
  def setup
    @entry = [ScribeThrift::LogEntry.new(:message => "something", :category => "thrift_client")]
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
    @socket = 1461
    @timeout = 0.2
    @options = {:protocol_extra_params => [false]}
  end

  def test_live_server
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers.last, @options).Log(@entry)
    end
  end

  def test_non_random_fall_through
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers, @options.merge(:randomize_server_list => false)).Log(@entry)
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

  def test_random_server_list
    @lists = []
    @lists << ThriftClient.new(ScribeThrift::Client, @servers, @options).server_list while @lists.size < 10
    assert @lists.uniq.size > 1
  end
  
  def test_random_fall_through
    assert_nothing_raised do
      10.times { ThriftClient.new(ScribeThrift::Client, @servers, @options).Log(@entry) }
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
  
  private
  
  def stub_server(port)
    socket = TCPServer.new('127.0.0.1', port)
    Thread.new { socket.accept }
    yield socket
  ensure
    socket.close
  end  
end