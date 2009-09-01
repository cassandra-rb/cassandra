require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class ComparableTypesTest < Test::Unit::TestCase
  include Cassandra::Constants

  def test_long_sort
    ary = []
    10.times { ary << Long.new }
    assert_equal ary.sort, ary
  end
  
  def test_long_equality
    long = Long.new
    assert_equal long, Long.new(long)  
    assert_equal long, Long.new(long.to_s)  
    assert_equal long, Long.new(long.to_i)  
    assert_equal long, Long.new(long.to_guid)  
  end
  
  def test_long_error
    assert_raises(Cassandra::Comparable::TypeError) do
      Long.new("bogus")
    end
  end

  def test_uuid_sort
    ary = []
    5.times { ary << UUID.new(Time.at(rand(2**31))) }
    assert_equal ary.map { |_| _.seconds }.sort, ary.sort.map { |_| _.seconds }
    assert_not_equal ary.sort, ary.sort_by {|_| _.to_guid }
  end
  
  def test_uuid_equality
    uuid = UUID.new
    assert_equal uuid, UUID.new(uuid)  
    assert_equal uuid, UUID.new(uuid.to_s)  
    assert_equal uuid, UUID.new(uuid.to_i)  
    assert_equal uuid, UUID.new(uuid.to_guid)  
  end
  
  def test_uuid_error
    assert_raises(Cassandra::Comparable::TypeError) do
      UUID.new("bogus")
    end
  end
end