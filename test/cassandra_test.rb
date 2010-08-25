require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class CassandraTest < Test::Unit::TestCase
  include Cassandra::Constants

  def setup
    @twitter = Cassandra.new('Twitter', "127.0.0.1:9160", :retries => 2, :exception_classes => [])
    @twitter.clear_keyspace!

    @blogs = Cassandra.new('Multiblog')
    @blogs.clear_keyspace!

    @blogs_long = Cassandra.new('MultiblogLong')
    @blogs_long.clear_keyspace!

    @uuids = (0..6).map {|i| SimpleUUID::UUID.new(Time.at(2**(24+i))) }
    @longs = (0..6).map {|i| Long.new(Time.at(2**(24+i))) }
  end

  def test_inspect
    assert_nothing_raised do
      @blogs.inspect
      @twitter.inspect
    end
  end

  def test_get_key
    @twitter.insert(:Users, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Users, key))
    assert_equal(['body', 'user'].sort, @twitter.get(:Users, key).timestamps.keys.sort)
    assert_equal({}, @twitter.get(:Users, 'bogus'))
  end

  def test_get_key_preserving_order
    # In-order hash is preserved
    hash = OrderedHash['a', '', 'b', '', 'c', '', 'd', '',]
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys, @twitter.get(:Users, key).keys)

    @twitter.remove(:Users, key)

    # Out-of-order hash is returned sorted
    hash = OrderedHash['b', '', 'c', '', 'd', '', 'a', '']
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys.sort, @twitter.get(:Users, key).keys)
    assert_equal(hash.timestamps.keys.sort, @twitter.get(:Users, key).timestamps.keys.sort)
    assert_not_equal(hash.keys, @twitter.get(:Users, key).keys)
  end

  def test_get_first_time_uuid_column
    @blogs.insert(:Blogs, key,
      {@uuids[0] => 'I like this cat', @uuids[1] => 'Buttons is cuter', @uuids[2] => 'I disagree'})

    assert_equal({@uuids[0] => 'I like this cat'}, @blogs.get(:Blogs, key, :count => 1))
    assert_equal({@uuids[2] => 'I disagree'}, @blogs.get(:Blogs, key, :count => 1, :reversed => true))
    assert_equal({}, @blogs.get(:Blogs, 'bogus'))
  end

  def test_get_multiple_time_uuid_columns
    @blogs.insert(:Blogs, key,
      {@uuids[0] => 'I like this cat', @uuids[1] => 'Buttons is cuter', @uuids[2] => 'I disagree'})

    assert_equal(['I like this cat', 'Buttons is cuter'], @blogs.get_columns(:Blogs, key, @uuids[0..1]))
  end

  def test_get_first_long_column
    @blogs_long.insert(:Blogs, key,
      {@longs[0] => 'I like this cat', @longs[1] => 'Buttons is cuter', @longs[2] => 'I disagree'})

    assert_equal({@longs[0] => 'I like this cat'}, @blogs_long.get(:Blogs, key, :count => 1))
    assert_equal({@longs[2] => 'I disagree'}, @blogs_long.get(:Blogs, key, :count => 1, :reversed => true))
    assert_equal({}, @blogs_long.get(:Blogs, 'bogus'))

    assert_equal([@longs[0]], @blogs_long.get(:Blogs, key, :count => 1).timestamps.keys)
    assert_equal([@longs[2]], @blogs_long.get(:Blogs, key, :count => 1, :reversed => true).timestamps.keys)
  end

  def test_long_remove_bug
    @blogs_long.insert(:Blogs, key, {@longs[0] => 'I like this cat'})
    @blogs_long.remove(:Blogs, key)
    assert_equal({}, @blogs_long.get(:Blogs, key, :count => 1))

    @blogs_long.insert(:Blogs, key, {@longs[0] => 'I really like this cat'})
    assert_equal({@longs[0] => 'I really like this cat'}, @blogs_long.get(:Blogs, key, :count => 1))
    assert_equal([@longs[0]], @blogs_long.get(:Blogs, key, :count => 1).timestamps.keys)
  end

  def test_get_with_count
    @twitter.insert(:Statuses, key, {'1' => 'v', '2' => 'v', '3' => 'v'})
    assert_equal 1, @twitter.get(:Statuses, key, :count => 1).size
    assert_equal 2, @twitter.get(:Statuses, key, :count => 2).size
    assert_equal 1, @twitter.get(:Statuses, key, :count => 1).timestamps.size
    assert_equal 2, @twitter.get(:Statuses, key, :count => 2).timestamps.size
  end

  def test_get_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal 'v', @twitter.get(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, 'bogus', 'body')

    assert @twitter.exists?(:Statuses, key, 'body')
    assert !@twitter.exists?(:Statuses, 'bogus', 'body')
  end

  def test_exists_with_only_key
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert @twitter.exists?(:Statuses, key)
  end

  def test_get_super_key
    columns = {'user_timelines' => {@uuids[4] => '4', @uuids[5] => '5'}}
    @twitter.insert(:StatusRelationships, key, columns)
    assert_equal(columns, @twitter.get(:StatusRelationships, key))
    assert_equal(columns.keys, @twitter.get(:StatusRelationships, key).timestamps.keys)
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_several_super_keys
    columns = {
      'user_timelines' => {@uuids[1]  => 'v1'},
      'mentions_timelines' => {@uuids[2]  => 'v2'}}
    @twitter.insert(:StatusRelationships, key, columns)

    assert_equal(columns, @twitter.get(:StatusRelationships, key))
    assert_equal(columns.keys, @twitter.get(:StatusRelationships, key).timestamps.keys)
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_super_sub_keys_with_count
    @twitter.insert(:StatusRelationships, key,
      {'user_timelines' => {@uuids[1]  => 'v1', @uuids[2]  => 'v2', @uuids[3]  => 'v3'}})
    assert_equal({@uuids[1]  => 'v1'},
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1))
    assert_equal({@uuids[3]  => 'v3'},
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1, :reversed => true))
    assert_equal([@uuids[1]],
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1).timestamps.keys)
    assert_equal([@uuids[3]],
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1, :reversed => true).timestamps.keys)
  end

  def test_get_super_sub_keys_with_ranges
    @twitter.insert(:StatusRelationships, key,
      {'user_timelines' => {
        @uuids[1] => 'v1',
        @uuids[2] => 'v2',
        @uuids[3] => 'v3',
        @uuids[4] => 'v4',
        @uuids[5] => 'v5'}})

    keys = @twitter.get(:StatusRelationships, key, "user_timelines").keys
    assert_equal keys.sort, keys
    assert_equal({@uuids[1] => 'v1'}, @twitter.get(:StatusRelationships, key, "user_timelines", :finish => @uuids[2], :count => 1))
    assert_equal({@uuids[2] => 'v2'}, @twitter.get(:StatusRelationships, key, "user_timelines", :start => @uuids[2], :count => 1))
    assert_equal 4, @twitter.get(:StatusRelationships, key, "user_timelines", :start => @uuids[2], :finish => @uuids[5]).size
    assert_equal([@uuids[1]], @twitter.get(:StatusRelationships, key, "user_timelines", :finish => @uuids[2], :count => 1).timestamps.keys)
    assert_equal([@uuids[2]], @twitter.get(:StatusRelationships, key, "user_timelines", :start => @uuids[2], :count => 1).timestamps.keys)
    assert_equal 4, @twitter.get(:StatusRelationships, key, "user_timelines", :start => @uuids[2], :finish => @uuids[5]).timestamps.size
  end

  def test_get_super_sub_key
    columns = {@uuids[1] => 'v1', @uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal(columns, @twitter.get(:StatusRelationships, key, 'user_timelines'))
    assert_equal(columns.keys.sort, @twitter.get(:StatusRelationships, key, 'user_timelines').timestamps.keys.sort)
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus', 'user_timelines'))
    # FIXME Not sure if this is valid
    # assert_nil @twitter.exists?(:StatusRelationships, 'bogus', 'user_timelines')
  end

  def test_get_super_value
    columns = {@uuids[1] => 'v1'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal('v1', @twitter.get(:StatusRelationships, key, 'user_timelines', columns.keys.first))
    assert_nil @twitter.get(:StatusRelationships, 'bogus', 'user_timelines', columns.keys.first)
  end


  #TODO: add a OPP keyspace for this
  # def test_get_range
  #   @twitter.insert(:Statuses, '2', {'body' => '1'})
  #   @twitter.insert(:Statuses, '3', {'body' => '1'})
  #   @twitter.insert(:Statuses, '4', {'body' => '1'})
  #   @twitter.insert(:Statuses, '5', {'body' => '1'})
  #   @twitter.insert(:Statuses, '6', {'body' => '1'})
  #   assert_equal(['3', '4', '5'], @twitter.get_range(:Statuses, :start => '3', :finish => '5'))
  # end

  def test_get_range_count
     @twitter.insert(:Statuses, '2', {'body' => '1'})
     @twitter.insert(:Statuses, '3', {'body' => '1'})
     @twitter.insert(:Statuses, '4', {'body' => '1'})
     @twitter.insert(:Statuses, '5', {'body' => '1'})
     @twitter.insert(:Statuses, '6', {'body' => '1'})
     assert_equal(3, @twitter.get_range(:Statuses, :count => 3).size)
  end

  def test_multi_get
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})

    expected = OrderedHash[key + '1', {'body' => 'v1', 'user' => 'v1'}, key + '2', {'body' => 'v2', 'user' => 'v2'}, 'bogus', {}]
    result = @twitter.multi_get(:Users, [key + '1', key + '2', 'bogus'])
    assert_equal expected, result
    assert_equal expected.keys, result.keys
    assert_equal expected.keys.sort, @twitter.multi_get(:Users, [key + '1', key + '2', 'bogus']).timestamps.keys.sort

    expected = OrderedHash[key + '2', {'body' => 'v2', 'user' => 'v2'}, 'bogus', {}, key + '1', {'body' => 'v1', 'user' => 'v1'}]
    result = @twitter.multi_get(:Users, [key + '2', 'bogus', key + '1'])
    assert_equal expected, result
    assert_equal expected.keys, result.keys
    assert_equal expected.keys.sort, @twitter.multi_get(:Users, [key + '2', 'bogus', key + '1']).timestamps.keys.sort
  end

  def test_remove_key
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal({'body' => 'v'}, @twitter.get(:Statuses, key))

    @twitter.remove(:Statuses, key)
    assert_equal({}, @twitter.get(:Statuses, key))
  end

  def test_remove_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    @twitter.remove(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, key).timestamps['body']
  end

  def test_remove_super_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1'}})
    @twitter.remove(:StatusRelationships, key)
    assert_equal({}, @twitter.get(:StatusRelationships, key))
  end

  def test_remove_super_sub_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1'}})
    @twitter.remove(:StatusRelationships, key, 'user_timelines')
    assert_equal({}, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_remove_super_value
    columns = {@uuids[1] => 'v1'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    @twitter.remove(:StatusRelationships, key, 'user_timelines', columns.keys.first)
    assert_nil @twitter.get(:StatusRelationships, key, 'user_timelines', columns.keys.first)
    assert_nil @twitter.get(:StatusRelationships, key, 'user_timelines').timestamps[columns.keys.first]
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
    assert_equal(['body', 'user'], @twitter.get(:Statuses, key).timestamps.keys)
  end

  def test_insert_super_key
    columns = {@uuids[1] => 'v1', @uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => columns})
    assert_equal(columns, @twitter.get(:StatusRelationships, key, 'user_timelines'))
    assert_equal(columns.keys.sort, @twitter.get(:StatusRelationships, key, 'user_timelines').timestamps.keys.sort)
  end

  def test_get_columns
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @twitter.get_columns(:Statuses, key, ['body', 'user']))
  end

  def test_get_column_values_super
    user_columns, mentions_columns = {@uuids[1] => 'v1'}, {@uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key,
      {'user_timelines' => user_columns, 'mentions_timelines' => mentions_columns})
    assert_equal [user_columns, mentions_columns],
      @twitter.get_columns(:StatusRelationships, key, ['user_timelines', 'mentions_timelines'])
  end

  def test_get_sub_column_values_super
    user_columns = {@uuids[1] => 'v1', @uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => user_columns})
    assert_equal ['v1', 'v2'],
      @twitter.get_columns(:StatusRelationships, key, 'user_timelines', @uuids[1..2])
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
    assert_equal(
      OrderedHash[key + '1', ['v1', 'v1'], key + '2', ['v2', 'v2'], 'bogus', [nil, nil]].keys.sort,
      @twitter.multi_get_columns(:Users, [key + '1', key + '2', 'bogus'], ['body', 'user']).timestamps.keys.sort)
    assert_equal(
      OrderedHash[key + '2', ['v2', 'v2'], 'bogus', [nil, nil], key + '1', ['v1', 'v1']].keys.sort,
      @twitter.multi_get_columns(:Users, [key + '2', 'bogus', key + '1'], ['body', 'user']).timestamps.keys.sort)
  end

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
      'user_timelines' => {@uuids[1] => 'v1'},
      'mentions_timelines' => {@uuids[2] => 'v2'}})
    assert_equal 2, @twitter.count_columns(:StatusRelationships, key)
  end

  def test_count_super_sub_columns
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1', @uuids[2] => 'v2'}})
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

  def test_batch_mutate
    k = key

    @twitter.insert(:Users, k + '1', {'body' => 'v1', 'user' => 'v1'})

    @twitter.batch do
      @twitter.insert(:Users, k + '2', {'body' => 'v2', 'user' => 'v2'})
      @twitter.insert(:Users, k + '3', {'body' => 'bogus', 'user' => 'v3'})
      @twitter.insert(:Users, k + '3', {'body' => 'v3', 'location' => 'v3'})
      @twitter.insert(:Statuses, k + '3', {'body' => 'v'})

      assert_equal({'body' => 'v1', 'user' => 'v1'}, @twitter.get(:Users, k + '1')) # Written
      assert_equal({}, @twitter.get(:Users, k + '2')) # Not yet written
      assert_equal({}, @twitter.get(:Statuses, k + '3')) # Not yet written

      @twitter.remove(:Users, k + '1')
      assert_equal({'body' => 'v1', 'user' => 'v1'}, @twitter.get(:Users, k + '1')) # Not yet removed

      @twitter.remove(:Users, k + '4')
      @twitter.insert(:Users, k + '4', {'body' => 'v4', 'user' => 'v4'})
      assert_equal({}, @twitter.get(:Users, k + '4')) # Not yet written
    end

    assert_equal({'body' => 'v2', 'user' => 'v2'}, @twitter.get(:Users, k + '2')) # Written
    assert_equal({'body' => 'v3', 'user' => 'v3', 'location' => 'v3'}, @twitter.get(:Users, k + '3')) # Written and compacted
    assert_equal({'body' => 'v4', 'user' => 'v4'}, @twitter.get(:Users, k + '4')) # Written
    assert_equal({'body' => 'v'}, @twitter.get(:Statuses, k + '3')) # Written
    assert_equal({}, @twitter.get(:Users, k + '1')) # Removed

    assert_equal({'body' => 'v2', 'user' => 'v2'}.keys.sort, @twitter.get(:Users, k + '2').timestamps.keys.sort) # Written
    assert_equal({'body' => 'v3', 'user' => 'v3', 'location' => 'v3'}.keys.sort, @twitter.get(:Users, k + '3').timestamps.keys.sort) # Written and compacted
    assert_equal({'body' => 'v4', 'user' => 'v4'}.keys.sort, @twitter.get(:Users, k + '4').timestamps.keys.sort) # Written
    assert_equal({'body' => 'v'}.keys.sort, @twitter.get(:Statuses, k + '3').timestamps.keys.sort) # Written
  end

  def test_complain_about_nil_key
    assert_raises(ArgumentError) do
      @twitter.insert(:Statuses, nil, {'text' => 'crap'})
    end
  end

  def test_raise_access_error_on_nonexistent_keyspace
    nonexistent = Cassandra.new('Nonexistent')
    assert_raises(Cassandra::AccessError) do
      nonexistent.get "foo", "bar"
    end
  end

  def test_nil_sub_column_value
    @twitter.insert(:Index, 'asdf', {"thing" => {'jkl' => ''} })
  end

  def test_disconnect!
    @twitter.disconnect!
    assert_nil @twitter.instance_variable_get(:@client)
  end

  def test_super_allows_for_non_string_values_while_normal_does_not
    columns = {'user_timelines' => {@uuids[4] => '4', @uuids[5] => '5'}}

    @twitter.insert(:StatusRelationships, key, columns)
    @twitter.insert(:Statuses, key, { 'body' => '1' })
  end

  private

  def key
    caller.first[/`(.*?)'/, 1]
  end
end
