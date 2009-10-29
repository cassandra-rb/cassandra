require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase
  def setup
    @entry = [ScribeThrift::LogEntry.new(:message => "something", :category => "thrift_client")]
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
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
    assert_raises(Thrift::TransportException) do
      ThriftClient.new(ScribeThrift::Client, @servers[0,2], @options).Log(@entry)
    end
  end

  def test_retry_period
    client = ThriftClient.new(ScribeThrift::Client, @servers[0,2], @options.merge(:server_retry_period => 1))
    assert_raises(Thrift::TransportException) { client.Log(@entry) }
    assert_raises(ThriftClient::NoServersAvailable) { client.Log(@entry) }
    sleep 1
    assert_raises(Thrift::TransportException) { client.Log(@entry) }
  end
end