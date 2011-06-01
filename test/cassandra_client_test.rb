require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class CassandraClientTest < Test::Unit::TestCase
  include Cassandra::Constants
  
  def setup
    @twitter = Cassandra.new('Twitter', "127.0.0.1:9160", :retries => 2, :exception_classes => [])
  end
  
  def test_client_method_is_called
    assert_nil @twitter.instance_variable_get(:@client)
    @twitter.insert(:Statuses, key, {'1' => 'v', '2' => 'v', '3' => 'v'})
    assert_not_nil @twitter.instance_variable_get(:@client)
  end
  
  def key
    caller.first[/`(.*?)'/, 1]
  end
  
end