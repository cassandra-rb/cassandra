
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra_client"

begin; require 'ruby-debug'; rescue LoadError; end

class CassandraClientTest < Test::Unit::TestCase
  def setup
    @client = CassandraClient.new('127.0.0.1')
    @statuses = @client.table('Statuses')
    @users = @client.table('Users')
    [@statuses, @users].each do |table|
      table.schema.keys.each do |column_family|
        table.get_key_range(column_family).each { |key| table.remove(column_family, key) }
      end
    end
  end
  
  def test_inspect
    assert_nothing_raised do
      @statuses.inspect
      @client.inspect
    end
  end

  def test_get_key_name_sorted
    @users.insert('Rows', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @users.get('Rows', key))
    assert_equal({}, @users.get('Rows', 'bogus'))
  end
  
  def test_get_key_name_sorted_preserving_order
    # In-order hash is preserved
    hash = CassandraClient::OrderedHash['a', '', 'b', '', 'c', '', 'd', '',]    
    @users.insert('Rows', key, hash)
    assert_equal(hash.keys, @users.get('Rows', key).keys)
    
    @users.remove('Rows', key)
        
    # Out-of-order hash is returned sorted
    hash = CassandraClient::OrderedHash['b', '', 'c', '', 'd', '', 'a', '']    
    @users.insert('Rows', key, hash)
    assert_equal(hash.keys.sort, @users.get('Rows', key).keys)
    assert_not_equal(hash.keys, @users.get('Rows', key).keys)
  end  

  def test_get_key_time_sorted
    @statuses.insert('Rows', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @statuses.get('Rows', key))
    assert_equal({}, @statuses.get('Rows', 'bogus'))
  end

  def test_get_value
    @statuses.insert('Rows', key, {'body' => 'v'})
    assert_equal 'v', @statuses.get('Rows', key, 'body')
    assert_nil @statuses.get('Rows', 'bogus', 'body')
  end
  
  def test_get_super_key
    @statuses.insert('Relationships', key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'user_timelines' => {'4' => 'v', '5' => 'v'}}, @statuses.get('Relationships', key))
    assert_equal({}, @statuses.get('Relationships', 'bogus'))
  end

  def test_get_super_key_multi
    @statuses.insert('Relationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal({
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}}, @statuses.get('Relationships', key))
    assert_equal({}, @statuses.get('Relationships', 'bogus'))
  end

  def test_get_super_sub_key
    @statuses.insert('Relationships', key, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'4' => 'v', '5' => 'v'}, @statuses.get('Relationships', key, 'user_timelines'))
    assert_equal({}, @statuses.get('Relationships', 'bogus', 'user_timelines'))
  end
  
  def test_get_super_value
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v'}})
    assert_equal('v', @statuses.get('Relationships', key, 'user_timelines', '1'))
    assert_nil @statuses.get('Relationships', 'bogus', 'user_timelines', '1')
  end  
  
  def test_get_key_range
    @statuses.insert('Rows', '3', {'body' => 'v'})
    @statuses.insert('Rows', '4', {'body' => 'v'})
    @statuses.insert('Rows', '5', {'body' => 'v'})
    assert_equal(['3', '4', '5'], @statuses.get_key_range('Rows', '3'..'5'))
  end

  # Not supported
  #  def test_get_key_range_super
  #    @statuses.insert('Relationships', '3', {'user_timelines' => {'1' => 'v'}})
  #    @statuses.insert('Relationships', '4', {'user_timelines' => {'1' => 'v'}})
  #    @statuses.insert('Relationships', '5', {'user_timelines' => {'1' => 'v'}})
  #    assert_equal(['3', '4', '5'], @statuses.get_key_range('Relationships', '3'..'5', 'user_timelines'))
  #  end
  
  def test_remove_key
    @statuses.insert('Rows', key, {'body' => 'v'})
    @statuses.remove('Rows', key)
    assert_equal({}, @statuses.get('Rows', key))
  end

  def test_remove_value
    @statuses.insert('Rows', key, {'body' => 'v'})
    @statuses.remove('Rows', key, 'body')
    assert_nil @statuses.get('Rows', key, 'body')    
  end

  def test_remove_super_key
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove('Relationships', key)
    assert_equal({}, @statuses.get('Relationships', key))
  end

  def test_remove_super_sub_key
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove('Relationships', key, 'user_timelines')
    assert_equal({}, @statuses.get('Relationships', key, 'user_timelines'))
  end

  def test_remove_super_value
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove('Relationships', key, 'user_timelines', '1')
    assert_nil @statuses.get('Relationships', key, 'user_timelines', '1')    
  end

  def test_insert_key
    @statuses.insert('Rows', key, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @statuses.get('Rows', key))  
  end

  def test_insert_super_key
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v', key => 'v'}})
    assert_equal({'1' => 'v' , key => 'v'}, @statuses.get('Relationships', key, 'user_timelines'))  
  end
  
  def test_get_column_values
    @statuses.insert('Rows', key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @statuses.get_columns('Rows', key, ['body', 'user']))
  end  

  def test_get_column_values_super
    @statuses.insert('Relationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal [{'1' => 'v1'}, {'2' => 'v2'}], 
      @statuses.get_columns('Relationships', key, ['user_timelines', 'mentions_timelines'])
  end  

  # Not supported
  #  def test_get_columns_super_sub
  #    @statuses.insert('Relationships', key, {
  #      'user_timelines' => {'1' => 'v1'}, 
  #      'mentions_timelines' => {'2' => 'v2'}})
  #    assert_equal ['v1', 'v2'], 
  #      @statuses.get_columns('Relationships', key, 'user_timelines', ['1', key])
  #  end    
  
  def test_count_keys
    @statuses.insert('Rows', key, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @statuses.count('Rows', key)
  end 

  def test_count_super_keys
    @statuses.insert('Relationships', key, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal 2, @statuses.count('Relationships', key)
  end 

  def test_count_super_sub_keys
    @statuses.insert('Relationships', key, {'user_timelines' => {'1' => 'v1', key => 'v2'}})
    assert_equal 2, @statuses.count('Relationships', key, 'user_timelines')
  end
  
  def key
    caller.first[/`(.*?)'/, 1]
  end
end
