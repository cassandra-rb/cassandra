
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra"

begin; require 'ruby-debug'; rescue LoadError; end

class CassandraTest < Test::Unit::TestCase
  include Cassandra::Constants

  def setup
    @twitter = Cassandra.new('Twitter', '127.0.0.1')
    @twitter.clear_keyspace!
    @blogs = Cassandra.new('Multiblog', '127.0.0.1')
    @blogs.clear_keyspace!
  end

  def test_inspect
    assert_nothing_raised do
      @blogs.inspect
      @twitter.inspect
    end
  end

  def test_connection_reopens
    assert_raises(NoMethodError) do
      @twitter.insert(:Statuses, 1, {'body' => 'v'})
    end
    assert_nothing_raised do
      @twitter.insert(:Statuses, key, {'body' => 'v'})
    end
  end

  def test_get_key_name_sorted
    @twitter.insert(:Users, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Users, key))
    assert_equal({}, @twitter.get(:Users, 'bogus'))
  end

  def test_get_key_name_sorted_preserving_order
    # In-order hash is preserved
    hash = OrderedHash['a', '', 'b', '', 'c', '', 'd', '',]
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys, @twitter.get(:Users, key).keys)

    @twitter.remove(:Users, key)

    # Out-of-order hash is returned sorted
    hash = OrderedHash['b', '', 'c', '', 'd', '', 'a', '']
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys.sort, @twitter.get(:Users, key).keys)
    assert_not_equal(hash.keys, @twitter.get(:Users, key).keys)
  end

  def test_get_key_time_sorted
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))
    assert_equal({}, @twitter.get(:Statuses, 'bogus'))
  end

  def test_get_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal 'v', @twitter.get(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, 'bogus', 'body')

    assert @twitter.exists?(:Statuses, key, 'body')
    assert_nil @twitter.exists?(:Statuses, 'bogus', 'body')
  end

  def test_get_super_key
    columns = {'user_timelines' => {UUID.new => '4', UUID.new => '5'}}
    @twitter.insert(:StatusRelationships, key, columns)
    assert_equal(columns, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_several_super_keys
    columns = {
      'user_timelines' => {UUID.new => 'v1'},
      'mentions_timelines' => {UUID.new => 'v2'}}
    @twitter.insert(:StatusRelationships, key, columns)

    assert_equal(columns, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_super_sub_keys_with_count
    columns = {UUID.new => 'v1'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v2'}})
    assert_equal(columns, @twitter.get(:StatusRelationships, key, "user_timelines", nil, 1))
  end

  def test_get_super_sub_keys_with_ranges
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v1'}})
    first_column = {UUID.new => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => first_column})
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v3', UUID.new => 'v4'}})    
    last_column = {UUID.new => 'v5'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => last_column})
    
    keys = @twitter.get(:StatusRelationships, key, "user_timelines").keys
    assert_equal keys.sort, keys
    
    assert_equal(first_column, @twitter.get(:StatusRelationships, key, "user_timelines", nil, 1, first_column.keys.first..''))
    assert_equal(3, @twitter.get(:StatusRelationships, key, "user_timelines", nil, 100, last_column.keys.first..first_column.keys.first).size)
  end

  def test_get_super_sub_key
    columns = {UUID.new => 'v', UUID.new => 'v'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal(columns, @twitter.get(:StatusRelationships, key, 'user_timelines'))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus', 'user_timelines'))
  end

  def test_get_super_value
    columns = {UUID.new => 'v'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal('v', @twitter.get(:StatusRelationships, key, 'user_timelines', columns.keys.first))
    assert_nil @twitter.get(:StatusRelationships, 'bogus', 'user_timelines', columns.keys.first)
  end

  def test_get_range
    @twitter.insert(:Statuses, '2', {'body' => '1'})
    @twitter.insert(:Statuses, '3', {'body' => '1'})
    @twitter.insert(:Statuses, '4', {'body' => '1'})
    @twitter.insert(:Statuses, '5', {'body' => '1'})
    @twitter.insert(:Statuses, '6', {'body' => '1'})
    assert_equal(['3', '4', '5'], @twitter.get_range(:Statuses, '3'..'5'))
  end

  def test_multi_get
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
    assert_equal(
      OrderedHash[key + '1', {'body' => 'v1', 'user' => 'v1'}, key + '2', {'body' => 'v2', 'user' => 'v2'}, 'bogus', {}],
      @twitter.multi_get(:Users, [key + '1', key + '2', 'bogus']))
    assert_equal(
      OrderedHash[key + '2', {'body' => 'v2', 'user' => 'v2'}, 'bogus', {}, key + '1', {'body' => 'v1', 'user' => 'v1'}],
      @twitter.multi_get(:Users, [key + '2', 'bogus', key + '1']))
  end

  def test_remove_key
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal({'body' => 'v'}, @twitter.get(:Statuses, key))

    @twitter.remove(:Statuses, key)
    assert_equal({}, @twitter.get(:Statuses, key))
    assert_equal 0, @twitter.count_range(:Statuses)
  end

  def test_remove_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    @twitter.remove(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, key, 'body')
  end

  def test_remove_super_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v'}})
    @twitter.remove(:StatusRelationships, key)
    assert_equal({}, @twitter.get(:StatusRelationships, key))
  end

  def test_remove_super_sub_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v'}})
    @twitter.remove(:StatusRelationships, key, 'user_timelines')
    assert_equal({}, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_remove_super_value
    columns = {UUID.new => 'v'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    @twitter.remove(:StatusRelationships, key, 'user_timelines', columns.keys.first)
    assert_nil @twitter.get(:StatusRelationships, key, 'user_timelines', columns.keys.first)
  end

  def test_clear_column_family
    @twitter.insert(:Statuses, key + "1", {'body' => '1'})
    @twitter.insert(:Statuses, key + "2", {'body' => '2'})
    @twitter.insert(:Statuses, key + "3", {'body' => '3'})
    @twitter.clear_column_family!(:Statuses)
    assert_equal 0, @twitter.count_range(:Statuses)
  end

  def test_insert_key
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))
  end

  def test_insert_super_key
    columns = {UUID.new => 'v', UUID.new => 'v'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal(columns, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_get_columns
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @twitter.get_columns(:Statuses, key, ['body', 'user']))
  end

  def test_get_column_values_super
    user_columns, mentions_columns = {UUID.new => 'v1'}, {UUID.new => 'v2'}
    @twitter.insert(:StatusRelationships, key,
      {'user_timelines' => user_columns, 'mentions_timelines' => mentions_columns})
    assert_equal [user_columns, mentions_columns],
      @twitter.get_columns(:StatusRelationships, key, ['user_timelines', 'mentions_timelines'])
  end

  def test_multi_get_columns
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
    assert_equal(
      OrderedHash[key + '1', ['v1', 'v1'], key + '2', ['v2', 'v2'], 'bogus', [nil, nil]],
      @twitter.multi_get_columns(:Users, [key + '1', key + '2', 'bogus'], ['body', 'user']))
    assert_equal(
      OrderedHash[key + '2', ['v2', 'v2'], 'bogus', [nil, nil], key + '1', ['v1', 'v1']],
      @twitter.multi_get_columns(:Users, [key + '2', 'bogus', key + '1'], ['body', 'user']))
  end

  # Not supported
  #  def test_get_columns_super_sub
  #    @twitter.insert(:StatusRelationships, key, {
  #      'user_timelines' => {UUID.new => 'v1'},
  #      'mentions_timelines' => {UUID.new => 'v2'}})
  #    assert_equal ['v1', 'v2'],
  #      @twitter.get_columns(:StatusRelationships, key, 'user_timelines', ['1', key])
  #  end

  def test_count_keys
    @twitter.insert(:Statuses, key + "1", {'body' => '1'})
    @twitter.insert(:Statuses, key + "2", {'body' => '2'})
    @twitter.insert(:Statuses, key + "3", {'body' => '3'})
    assert_equal 3, @twitter.count_range(:Statuses)
  end

  def test_count_columns
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @twitter.count_columns(:Statuses, key)
  end

  def test_count_super_columns
    @twitter.insert(:StatusRelationships, key, {
      'user_timelines' => {UUID.new => 'v1'},
      'mentions_timelines' => {UUID.new => 'v2'}})
    assert_equal 2, @twitter.count_columns(:StatusRelationships, key)
  end

  def test_count_super_sub_columns
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {UUID.new => 'v1', UUID.new => 'v2'}})
    assert_equal 2, @twitter.count_columns(:StatusRelationships, key, 'user_timelines')
  end

  def test_multi_count_columns
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
    assert_equal(
      OrderedHash[key + '1', 2, key + '2', 2, 'bogus', 0],
      @twitter.multi_count_columns(:Users, [key + '1', key + '2', 'bogus']))
    assert_equal(
      OrderedHash[key + '2', 2, 'bogus', 0, key + '1', 2],
      @twitter.multi_count_columns(:Users, [key + '2', 'bogus', key + '1']))
  end

  def test_batch_insert
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})

    @twitter.batch do
      @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
      @twitter.insert(:Users, key + '3', {'body' => 'bogus', 'user' => 'v3'})
      @twitter.insert(:Users, key + '3', {'body' => 'v3', 'location' => 'v3'})
      @twitter.insert(:Statuses, key + '3', {'body' => 'v'})

      assert_equal({'body' => 'v1', 'user' => 'v1'}, @twitter.get(:Users, key + '1')) # Written
      assert_equal({}, @twitter.get(:Users, key + '2')) # Not yet written
      assert_equal({}, @twitter.get(:Statuses, key + '3')) # Not yet written

      @twitter.remove(:Users, key + '1')
      assert_equal({'body' => 'v1', 'user' => 'v1'}, @twitter.get(:Users, key + '1')) # Not yet removed

      @twitter.remove(:Users, key + '4')
      @twitter.insert(:Users, key + '4', {'body' => 'v4', 'user' => 'v4'})
      assert_equal({}, @twitter.get(:Users, key + '4')) # Not yet written
    end

    assert_equal({'body' => 'v2', 'user' => 'v2'}, @twitter.get(:Users, key + '2')) # Written
    assert_equal({'body' => 'v3', 'user' => 'v3', 'location' => 'v3'}, @twitter.get(:Users, key + '3')) # Written and compacted
    assert_equal({'body' => 'v4', 'user' => 'v4'}, @twitter.get(:Users, key + '4')) # Written
    assert_equal({'body' => 'v'}, @twitter.get(:Statuses, key + '3')) # Written
    assert_equal({}, @twitter.get(:Users, key + '1')) # Removed
  end

  private

  def key
    caller.first[/`(.*?)'/, 1]
  end
end
