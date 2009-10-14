require "#{File.dirname(__FILE__)}/test_helper"

class ThriftClientTest < Test::Unit::TestCase
  def setup
    @entry = [ScribeThrift::LogEntry.new(:message => "something", :category => "thrift_client")]
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
  end
  
  def test_live_server
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers.last).Log(@entry)
    end
  end

  def test_non_random_fall_through
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers, :randomize_server_list => false).Log(@entry)
    end
  end
  
  def test_random_server_list
    @lists = []
    @lists << ThriftClient.new(ScribeThrift::Client, @servers).server_list while @lists.size < 10
    assert @lists.uniq.size > 1
  end

  def test_random_fall_through
    assert_nothing_raised do
      10.times { ThriftClient.new(ScribeThrift::Client, @servers).Log(@entry) }
    end
  end
  
  def test_lazy_connection
    assert_nothing_raised do
      ThriftClient.new(ScribeThrift::Client, @servers[0,2])
    end  
  end
  
  def test_no_servers_eventually_raise
    assert_raises(Thrift::TransportException) do
      ThriftClient.new(ScribeThrift::Client, @servers[0,2]).Log(@entry)
    end
  end  
end