
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra_client"

begin; require 'ruby-debug'; rescue LoadError; end

class CassandraClientTest < Test::Unit::TestCase
  def setup
    @twitter = CassandraClient.new('Twitter', '127.0.0.1')
    @twitter.clear_keyspace!
    @blogs = CassandraClient.new('Multiblog', '127.0.0.1')
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
    hash = CassandraClient::OrderedHash['a', nil, 'b', nil, 'c', nil, 'd', nil,]    
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys, @twitter.get(:Users, key).keys)
    
    @twitter.remove(:Users, key)
        
    # Out-of-order hash is returned sorted
    hash = CassandraClient::OrderedHash['b', nil, 'c', nil, 'd', nil, 'a', nil]    
    @twitter.insert(:Users, key, hash)
    assert_equal(hash.keys.sort, @twitter.get(:Users, key).keys)
    assert_not_equal(hash.keys, @twitter.get(:Users, key).keys)
  end  

  def test_get_key_time_sorted
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))
    assert_equal({}, @twitter.get(:Statuses, 'bogus'))
  end
    
  def test_get_key_time_sorted_with_limit
    @twitter.insert(:Statuses, key, {'first' => 'v'})
    @twitter.insert(:Statuses, key, {'second' => 'v'})
    assert_equal({'second' => 'v'}, @twitter.get(:Statuses, key, nil, nil, 0, 1))
  end    

  def test_get_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    assert_equal 'v', @twitter.get(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, 'bogus', 'body')
  end
    
  def test_get_super_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'user_timelines' => {'4' => 'v', '5' => 'v'}}, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_super_key_multi
    @twitter.insert(:StatusRelationships, key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal({
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}}, @twitter.get(:StatusRelationships, key))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus'))
  end

  def test_get_super_sub_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'4' => 'v', '5' => 'v'}, @twitter.get(:StatusRelationships, key, 'user_timelines'))
    assert_equal({}, @twitter.get(:StatusRelationships, 'bogus', 'user_timelines'))
  end
  
  def test_get_super_value
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v'}})
    assert_equal('v', @twitter.get(:StatusRelationships, key, 'user_timelines', '1'))
    assert_nil @twitter.get(:StatusRelationships, 'bogus', 'user_timelines', '1')
  end  
  
  def test_get_key_range
    @twitter.insert(:Statuses, '2', {'body' => '1'})
    @twitter.insert(:Statuses, '3', {'body' => '1'})
    @twitter.insert(:Statuses, '4', {'body' => '1'})
    @twitter.insert(:Statuses, '5', {'body' => '1'})
    @twitter.insert(:Statuses, '6', {'body' => '1'})
    assert_equal(['3', '4', '5'], @twitter.get_key_range(:Statuses, '3'..'5'))
  end

  # Not supported
  #  def test_get_key_range_super
  #    @twitter.insert(:StatusRelationships, '2', {'user_timelines' => {'1' => 'v'}})
  #    @twitter.insert(:StatusRelationships, '3', {'user_timelines' => {'1' => 'v'}})
  #    @twitter.insert(:StatusRelationships, '4', {'user_timelines' => {'1' => 'v'}})
  #    @twitter.insert(:StatusRelationships, '5', {'user_timelines' => {'1' => 'v'}})
  #    @twitter.insert(:StatusRelationships, '6', {'user_timelines' => {'1' => 'v'}})
  #    assert_equal(['3', '4', '5'], @twitter.get_key_range(:StatusRelationships, '3'..'5', 'user_timelines'))
  #  end
  
  def test_remove_key
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    @twitter.remove(:Statuses, key)
    assert_equal({}, @twitter.get(:Statuses, key))
  end

  def test_remove_value
    @twitter.insert(:Statuses, key, {'body' => 'v'})
    @twitter.remove(:Statuses, key, 'body')
    assert_nil @twitter.get(:Statuses, key, 'body')    
  end

  def test_remove_super_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v'}})
    @twitter.remove(:StatusRelationships, key)
    assert_equal({}, @twitter.get(:StatusRelationships, key))
  end

  def test_remove_super_sub_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v'}})
    @twitter.remove(:StatusRelationships, key, 'user_timelines')
    assert_equal({}, @twitter.get(:StatusRelationships, key, 'user_timelines'))
  end

  def test_remove_super_value
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v'}})
    @twitter.remove(:StatusRelationships, key, 'user_timelines', '1')
    assert_nil @twitter.get(:StatusRelationships, key, 'user_timelines', '1')    
  end

  def test_insert_key
    @twitter.insert(:Statuses, key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @twitter.get(:Statuses, key))  
  end

  def test_insert_super_key
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v', key => 'v'}})
    assert_equal({'1' => 'v' , key => 'v'}, @twitter.get(:StatusRelationships, key, 'user_timelines'))  
  end
  
  def test_get_column_values
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @twitter.get_columns(:Statuses, key,['body', 'user']))
  end  

  def test_get_column_values_super
    @twitter.insert(:StatusRelationships, key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal [{'1' => 'v1'}, {'2' => 'v2'}], 
      @twitter.get_columns(:StatusRelationships, key, ['user_timelines', 'mentions_timelines'])
  end  

  # Not supported
  #  def test_get_columns_super_sub
  #    @twitter.insert(:StatusRelationships, key, {
  #      'user_timelines' => {'1' => 'v1'}, 
  #      'mentions_timelines' => {'2' => 'v2'}})
  #    assert_equal ['v1', 'v2'], 
  #      @twitter.get_columns(:StatusRelationships, key, 'user_timelines', ['1', key])
  #  end    
  
  def test_count_keys
    @twitter.insert(:Statuses, key + "1", {'body' => '1'})
    @twitter.insert(:Statuses, key + "2", {'body' => '2'})
    @twitter.insert(:Statuses, key + "3", {'body' => '3'})
    assert_equal 3, @twitter.count(:Statuses)  
  end
  
  def test_count_columns
    @twitter.insert(:Statuses, key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @twitter.count_columns(:Statuses, key)
  end 

  def test_count_super_columns
    @twitter.insert(:StatusRelationships, key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal 2, @twitter.count_columns(:StatusRelationships, key)
  end 

  def test_count_super_sub_columns
    @twitter.insert(:StatusRelationships, key, {'user_timelines' => {'1' => 'v1', key => 'v2'}})
    assert_equal 2, @twitter.count_columns(:StatusRelationships, key, 'user_timelines')
  end
  
  private
  
  def key
    caller.first[/`(.*?)'/, 1]
  end
end
