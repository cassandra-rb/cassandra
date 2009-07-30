require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra_client"

begin; require 'ruby-debug'; rescue LoadError; end

class ComparableTypesTest < Test::Unit::TestCase
  include CassandraClient::Constants

  def test_long_sort
    ary = []
    10.times { ary << Long.new }
    assert_equal ary.sort, ary
  end
  
  def test_long_equality
    long = Long.new
    assert_equal long, Long.new(long.to_s)  
    assert_equal long, Long.new(long.to_i)  
  end
  
  def test_long_error
    assert_raises(CassandraClient::Comparable::TypeError) do
      Long.new("bogus")
    end
  end

  def test_uuid_sort
    ary = []
    10.times { ary << UUID.new }
    assert_equal ary.sort, ary
  end
  
  def test_uuid_equality
    uuid = UUID.new
    assert_equal uuid, UUID.new(uuid.to_s)  
    assert_equal uuid, UUID.new(uuid.to_i)  
  end
  
  def test_uuid_error
    assert_raises(CassandraClient::Comparable::TypeError) do
      UUID.new("bogus")
    end
  end
end