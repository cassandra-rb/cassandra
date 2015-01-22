require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class ComparableTypesTest < Test::Unit::TestCase
  include TwitterCassandra::Constants

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
    assert_raises(TwitterCassandra::Comparable::TypeError) do
      Long.new("bogus")
    end
  end

  def test_types_behave_well
    assert !(Long.new() == false)
  end

  def test_casting_unknown_class
    assert_raises(TwitterCassandra::Comparable::TypeError) do
      TwitterCassandra::Long.new({})
    end
  end

  def test_long_inspect
    obj = Long.new("\000\000\000\000\000\000\000\000")
    if RUBY_VERSION < '1.9'
      assert_equal "<TwitterCassandra::Long##{obj.object_id} time: Thu Jan 01 00:00:00 UTC 1970, usecs: 0, jitter: 0, guid: 00000000-0000-0000>", obj.inspect
    else
      assert_equal "<TwitterCassandra::Long##{obj.object_id} time: 1970-01-01 00:00:00 UTC, usecs: 0, jitter: 0, guid: 00000000-0000-0000>", obj.inspect
    end
  end

end
