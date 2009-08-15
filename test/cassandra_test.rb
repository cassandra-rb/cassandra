require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class CassandraTest < Test::Unit::TestCase
  include Cassandra::Constants

  def setup
    @twitter = Cassandra.new('Twitter', '127.0.0.1')
    @twitter.clear_database!
    @blogs = Cassandra.new('Multiblog', '127.0.0.1')
    @blogs.clear_database!
    @uuids = (0..6).map {|i| UUID.new(Time.at(2**(24+i))) }    
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

  def test_get_row_name_sorted
    @twitter.insert(:Users, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Users, key))
    assert_equal({}, @twitter.get(:Users, 'bogus'))
  end

  def test_get_row_name_sorted_preserving_order
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

  def test_get_row_time_sorted
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))
    assert_equal({}, @twitter.get(:Statuses, 'bogus'))
  end

  def test_get_with_count
    @twitter.insert(:Statuses, key, {'1' => 'v', '2' => 'v', '3' => 'v'})
    assert_equal 1, @twitter.get(:Statuses, key, :count => 1).size
    assert_equal 2, @twitter.get(:Statuses, key, :count => 2).size
  end  

  def test_get_field_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal 'v', @twitter.get(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, 'bogus', 'body')

    assert @twitter.exists?(:Statuses, key, 'body')
    assert_nil @twitter.exists?(:Statuses, 'bogus', 'body')
  end
  
  def test_get_field_set
    fields = {'user_timelines' => {@uuids[4] => '4', @uuids[5] => '5'}}
    @twitter.insert(:StatusRelationships, key, fields)
    assert_equal(fields, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_several_field_sets
    fields = {
      'user_timelines' => {@uuids[1]  => 'v1'},
      'mentions_timelines' => {@uuids[2]  => 'v2'}}
    @twitter.insert(:StatusRelationships, key, fields)

    assert_equal(fields, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_field_set_fields_with_count
    @twitter.insert(:StatusRelationships, key, 
      {'user_timelines' => {@uuids[1]  => 'v1', @uuids[2]  => 'v2', @uuids[3]  => 'v3'}})
    assert_equal({@uuids[1]  => 'v1'}, 
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1))
    assert_equal({@uuids[3]  => 'v3'}, 
      @twitter.get(:StatusRelationships, key, "user_timelines", :count => 1, :reversed => true))
  end

  def test_get_field_set_fields_with_ranges              
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
  end

  def test_get_field_set_field
    fields = {@uuids[1] => 'v1', @uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => fields})
    assert_equal(fields, @twitter.get(:StatusRelationships, key, 'user_timelines'))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus', 'user_timelines'))
  end

  def test_get_field_set
    fields = {@uuids[1] => 'v1'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => fields})
    assert_equal('v1', @twitter.get(:StatusRelationships, key, 'user_timelines', fields.keys.first))
    assert_nil @twitter.get(:StatusRelationships, 'bogus', 'user_timelines', fields.keys.first)
  end

  def test_get_range
    @twitter.insert(:Statuses, '2', {'body' => '1'})
    @twitter.insert(:Statuses, '3', {'body' => '1'})
    @twitter.insert(:Statuses, '4', {'body' => '1'})
    @twitter.insert(:Statuses, '5', {'body' => '1'})
    @twitter.insert(:Statuses, '6', {'body' => '1'})
    assert_equal(['3', '4', '5'], @twitter.get_range(:Statuses, :start => '3', :finish => '5'))
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

  def test_remove_row
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal({'body' => 'v'}, @twitter.get(:Statuses, key))

    @twitter.remove(:Statuses, key)
    assert_equal({}, @twitter.get(:Statuses, key))
    assert_equal 0, @twitter.count_range(:Statuses)
  end

  def test_remove_field
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    @twitter.remove(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, key, 'body')
  end

  def test_remove_field_set
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1'}})
    @twitter.remove(:StatusRelationships, key)
    assert_equal({}, @twitter.get(:StatusRelationships, key))
  end

  def test_remove_field_set_field
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1'}})
    @twitter.remove(:StatusRelationships, key, 'user_timelines')
    assert_equal({}, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_remove_field_set
    fields = {@uuids[1] => 'v1'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => fields})
    @twitter.remove(:StatusRelationships, key, 'user_timelines', fields.keys.first)
    assert_nil @twitter.get(:StatusRelationships, key, 'user_timelines', fields.keys.first)
  end

  def test_clear_row_set
    @twitter.insert(:Statuses, key + "1", {'body' => '1'})
    @twitter.insert(:Statuses, key + "2", {'body' => '2'})
    @twitter.insert(:Statuses, key + "3", {'body' => '3'})
    @twitter.clear_row_set!(:Statuses)
    assert_equal 0, @twitter.count_range(:Statuses)
  end

  def test_insert_row
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))
  end

  def test_insert_field_set
    fields = {@uuids[1] => 'v1', @uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => fields})
    assert_equal(fields, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_get_fields
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @twitter.get_fields(:Statuses, key, ['body', 'user']))
  end

  def test_get_field_set_values
    user_fields, mentions_fields = {@uuids[1] => 'v1'}, {@uuids[2] => 'v2'}
    @twitter.insert(:StatusRelationships, key,
      {'user_timelines' => user_fields, 'mentions_timelines' => mentions_fields})
    assert_equal [user_fields, mentions_fields],
      @twitter.get_fields(:StatusRelationships, key, ['user_timelines', 'mentions_timelines'])
  end

  def test_multi_get_fields
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
    assert_equal(
      OrderedHash[key + '1', ['v1', 'v1'], key + '2', ['v2', 'v2'], 'bogus', [nil, nil]],
      @twitter.multi_get_fields(:Users, [key + '1', key + '2', 'bogus'], ['body', 'user']))
    assert_equal(
      OrderedHash[key + '2', ['v2', 'v2'], 'bogus', [nil, nil], key + '1', ['v1', 'v1']],
      @twitter.multi_get_fields(:Users, [key + '2', 'bogus', key + '1'], ['body', 'user']))
  end

  def test_count_rows
    @twitter.insert(:Statuses, key + "1", {'body' => '1'})
    @twitter.insert(:Statuses, key + "2", {'body' => '2'})
    @twitter.insert(:Statuses, key + "3", {'body' => '3'})
    assert_equal 3, @twitter.count_range(:Statuses)
  end

  def test_count_fields
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @twitter.count_fields(:Statuses, key)
  end

  def test_count_field_sets
    @twitter.insert(:StatusRelationships, key, {
      'user_timelines' => {@uuids[1] => 'v1'},
      'mentions_timelines' => {@uuids[2] => 'v2'}})
    assert_equal 2, @twitter.count_fields(:StatusRelationships, key)
  end

  def test_count_field_set_fields
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {@uuids[1] => 'v1', @uuids[2] => 'v2'}})
    assert_equal 2, @twitter.count_fields(:StatusRelationships, key, 'user_timelines')
  end

  def test_multi_count_fields
    @twitter.insert(:Users, key + '1', {'body' => 'v1', 'user' => 'v1'})
    @twitter.insert(:Users, key + '2', {'body' => 'v2', 'user' => 'v2'})
    assert_equal(
      OrderedHash[key + '1', 2, key + '2', 2, 'bogus', 0],
      @twitter.multi_count_fields(:Users, [key + '1', key + '2', 'bogus']))
    assert_equal(
      OrderedHash[key + '2', 2, 'bogus', 0, key + '1', 2],
      @twitter.multi_count_fields(:Users, [key + '2', 'bogus', key + '1']))
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
