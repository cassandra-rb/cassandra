
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra_client"
require 'ruby-debug'

class CassandraClientTest < Test::Unit::TestCase
  def setup
    @c = CassandraClient.new('Twitter', '127.0.0.1')  
    @c.get_key_range('Statuses').each { |key| @c.remove('Statuses', key) }
    @c.get_key_range('StatusRelationships').each { |key| @c.remove('StatusRelationships', key) }
  end

  def test_get_key_time_sorted
    @c.insert('Statuses', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @c.get('Statuses', key))
    assert_equal({}, @c.get('Statuses', 'bogus'))
  end

  def test_get_key_name_sorted
    @c.insert('Users', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @c.get('Users', key))
    assert_equal({}, @c.get('Users', 'bogus'))
  end

  def test_get_value
    @c.insert('Statuses', key, {'body' => 'v'})
    assert_equal 'v', @c.get('Statuses', key, 'body')
    assert_nil @c.get('Statuses', 'bogus', 'body')
  end
  
  def test_get_super_key
    @c.insert('StatusRelationships', key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'user_timelines' => {'4' => 'v', '5' => 'v'}}, @c.get('StatusRelationships', key))
    assert_equal({}, @c.get('StatusRelationships', 'bogus'))
  end

  def test_get_super_key_multi
    @c.insert('StatusRelationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal({
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}}, @c.get('StatusRelationships', key))
    assert_equal({}, @c.get('StatusRelationships', 'bogus'))
  end

  def test_get_super_sub_key
    @c.insert('StatusRelationships', key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'4' => 'v', '5' => 'v'}, @c.get('StatusRelationships', key, 'user_timelines'))
    assert_equal({}, @c.get('StatusRelationships', 'bogus', 'user_timelines'))
  end
  
  def test_get_super_value
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v'}})
    assert_equal('v', @c.get('StatusRelationships', key, 'user_timelines', '1'))
    assert_nil @c.get('StatusRelationships', 'bogus', 'user_timelines', '1')
  end  
  
  def test_get_key_range
    @c.insert('Statuses', '3', {'body' => 'v'})
    @c.insert('Statuses', '4', {'body' => 'v'})
    @c.insert('Statuses', '5', {'body' => 'v'})
    assert_equal(['3', '4', '5'], @c.get_key_range('Statuses', '3'..'5'))
  end

  # Not supported
  #  def test_get_key_range_super
  #    @c.insert('StatusRelationships', '3', {'user_timelines' => {'1' => 'v'}})
  #    @c.insert('StatusRelationships', '4', {'user_timelines' => {'1' => 'v'}})
  #    @c.insert('StatusRelationships', '5', {'user_timelines' => {'1' => 'v'}})
  #    assert_equal(['3', '4', '5'], @c.get_key_range('StatusRelationships', '3'..'5', 'user_timelines'))
  #  end
  
  def test_remove_key
    @c.insert('Statuses', key, {'body' => 'v'})
    @c.remove('Statuses', key)
    assert_equal({}, @c.get('Statuses', key))
  end

  def test_remove_value
    @c.insert('Statuses', key, {'body' => 'v'})
    @c.remove('Statuses', key, 'body')
    assert_nil @c.get('Statuses', key, 'body')    
  end

  def test_remove_super_key
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v'}})
    @c.remove('StatusRelationships', key)
    assert_equal({}, @c.get('StatusRelationships', key))
  end

  def test_remove_super_sub_key
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v'}})
    @c.remove('StatusRelationships', key, 'user_timelines')
    assert_equal({}, @c.get('StatusRelationships', key, 'user_timelines'))
  end

  def test_remove_super_value
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v'}})
    @c.remove('StatusRelationships', key, 'user_timelines', '1')
    assert_nil @c.get('StatusRelationships', key, 'user_timelines', '1')    
  end

  def test_insert_key
    @c.insert('Statuses', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @c.get('Statuses', key))  
  end

  def test_insert_super_key
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v', key => 'v'}})
    assert_equal({'1' => 'v' , key => 'v'}, @c.get('StatusRelationships', key, 'user_timelines'))  
  end
  
  def test_get_column_values
    @c.insert('Statuses', key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @c.get_columns('Statuses', key, ['body', 'user']))
  end  

  def test_get_column_values_super
    @c.insert('StatusRelationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal [{'1' => 'v1'}, {'2' => 'v2'}], 
      @c.get_columns('StatusRelationships', key, ['user_timelines', 'mentions_timelines'])
  end  

  # Not supported
  #  def test_get_columns_super_sub
  #    @c.insert('StatusRelationships', key, {
  #      'user_timelines' => {'1' => 'v1'}, 
  #      'mentions_timelines' => {'2' => 'v2'}})
  #    assert_equal ['v1', 'v2'], 
  #      @c.get_columns('StatusRelationships', key, 'user_timelines', ['1', key])
  #  end    
  
  def test_count_keys
    @c.insert('Statuses', key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @c.count('Statuses', key)
  end 

  def test_count_super_keys
    @c.insert('StatusRelationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal 2, @c.count('StatusRelationships', key)
  end 

  def test_count_super_sub_keys
    @c.insert('StatusRelationships', key, {'user_timelines' => {'1' => 'v1', key => 'v2'}})
    assert_equal 2, @c.count('StatusRelationships', key, 'user_timelines')
  end
  
  def key
    caller.first[/`(.*?)'/, 1]
  end
end
