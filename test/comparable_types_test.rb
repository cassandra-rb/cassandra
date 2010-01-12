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

  def test_types_behave_well
    assert !(UUID.new() == false)
    assert !(Long.new() == false)
  end

  def test_casting_unknown_class
    assert_raises(Cassandra::Comparable::TypeError) do
      Cassandra::Long.new({})
    end
  end

  def test_inspect
    assert_equal '<Cassandra::Long#16395660 time: Wed Dec 31 16:00:00 -0800 1969, usecs: 0, jitter: 0, guid: 00000000-0000-0000>', Long.new("\000\000\000\000\000\000\000\000").inspect
  end
end