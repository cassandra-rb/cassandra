
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra_client"

begin; require 'ruby-debug'; rescue LoadError; end

class CassandraClientTest < Test::Unit::TestCase
  def setup
    @client = CassandraClient.new('127.0.0.1')
    @client.remove_all
    @statuses = @client.table('Statuses')
    @users = @client.table('Users')
  end
  
  def test_inspect
    assert_nothing_raised do
      @statuses.inspect
      @client.inspect
    end
  end
  
  def test_connection_reopens
    assert_raises(NoMethodError) do
      @statuses.insert(1, :row, {'body' => 'v'})
    end
    assert_nothing_raised do
      @statuses.insert(key, :row, {'body' => 'v'})
    end
  end  

  def test_get_key_name_sorted
    @users.insert(key, :row, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @users.get(key, :row))
    assert_equal({}, @users.get('bogus', :row))
  end
  
  def test_get_key_name_sorted_preserving_order
    # In-order hash is preserved
    hash = CassandraClient::OrderedHash['a', '', 'b', '', 'c', '', 'd', '',]    
    @users.insert(key, :row, hash)
    assert_equal(hash.keys, @users.get(key, :row).keys)
    
    @users.remove(key, :row)
        
    # Out-of-order hash is returned sorted
    hash = CassandraClient::OrderedHash['b', '', 'c', '', 'd', '', 'a', '']    
    @users.insert(key, :row, hash)
    assert_equal(hash.keys.sort, @users.get(key, :row).keys)
    assert_not_equal(hash.keys, @users.get(key, :row).keys)
  end  

  def test_get_key_time_sorted
    @statuses.insert(key, :row, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @statuses.get(key, :row))
    assert_equal({}, @statuses.get('bogus', :row))
  end
    
  def test_get_key_time_sorted_with_limit
    @statuses.insert(key, :row, {'first' => 'v'})
    @statuses.insert(key, :row, {'second' => 'v'})
    assert_equal({'second' => 'v'}, @statuses.get(key, :row, nil, nil, 0, 1))
  end    

  def test_get_value
    @statuses.insert(key, :row, {'body' => 'v'})
    assert_equal 'v', @statuses.get(key, :row, 'body')
    assert_nil @statuses.get('bogus', :row, 'body')
  end
    
  def test_get_super_key
    @statuses.insert(key, :relationships, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'user_timelines' => {'4' => 'v', '5' => 'v'}}, @statuses.get(key, :relationships))
    assert_equal({}, @statuses.get('bogus', :relationships))
  end

  def test_get_super_key_multi
    @statuses.insert(key, :relationships, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal({
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}}, @statuses.get(key, :relationships))
    assert_equal({}, @statuses.get('bogus', :relationships))
  end

  def test_get_super_sub_key
    @statuses.insert(key, :relationships, {'user_timelines' => {'4' => 'v', '5' => 'v'}})
    assert_equal({'4' => 'v', '5' => 'v'}, @statuses.get(key, :relationships, 'user_timelines'))
    assert_equal({}, @statuses.get('bogus', :relationships, 'user_timelines'))
  end
  
  def test_get_super_value
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v'}})
    assert_equal('v', @statuses.get(key, :relationships, 'user_timelines', '1'))
    assert_nil @statuses.get('bogus', :relationships, 'user_timelines', '1')
  end  
  
  def test_get_key_range
    @statuses.insert('3', :row, {'body' => 'v'})
    @statuses.insert('4', :row, {'body' => 'v'})
    @statuses.insert('5', :row, {'body' => 'v'})
    assert_equal(['3', '4', '5'], @statuses.get_key_range('3'..'5', :row))
  end

  # Not supported
  #  def test_get_key_range_super
  #    @statuses.insert('3', :relationships, {'user_timelines' => {'1' => 'v'}})
  #    @statuses.insert('4', :relationships, {'user_timelines' => {'1' => 'v'}})
  #    @statuses.insert('5', :relationships, {'user_timelines' => {'1' => 'v'}})
  #    assert_equal(['3', '4', '5'], @statuses.get_key_range('3'..'5', :relationships, 'user_timelines'))
  #  end
  
  def test_remove_key
    @statuses.insert(key, :row, {'body' => 'v'})
    @statuses.remove(key, :row)
    assert_equal({}, @statuses.get(key, :row))
  end

  def test_remove_value
    @statuses.insert(key, :row, {'body' => 'v'})
    @statuses.remove(key, :row, 'body')
    assert_nil @statuses.get(key, :row, 'body')    
  end

  def test_remove_super_key
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove(key, :relationships)
    assert_equal({}, @statuses.get(key, :relationships))
  end

  def test_remove_super_sub_key
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove(key, :relationships, 'user_timelines')
    assert_equal({}, @statuses.get(key, :relationships, 'user_timelines'))
  end

  def test_remove_super_value
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v'}})
    @statuses.remove(key, :relationships, 'user_timelines', '1')
    assert_nil @statuses.get(key, :relationships, 'user_timelines', '1')    
  end

  def test_insert_key
    @statuses.insert(key, :row, {'body' => 'v', 'user' => 'v'})
    assert_equal({'body' => 'v', 'user' => 'v'}, @statuses.get(key, :row))  
  end

  def test_insert_super_key
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v', key => 'v'}})
    assert_equal({'1' => 'v' , key => 'v'}, @statuses.get(key, :relationships, 'user_timelines'))  
  end
  
  def test_get_column_values
    @statuses.insert(key, :row, {'body' => 'v1', 'user' => 'v2'})
    assert_equal(['v1' , 'v2'], @statuses.get_columns(key, :row, ['body', 'user']))
  end  

  def test_get_column_values_super
    @statuses.insert(key, :relationships, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal [{'1' => 'v1'}, {'2' => 'v2'}], 
      @statuses.get_columns(key, :relationships, ['user_timelines', 'mentions_timelines'])
  end  

  # Not supported
  #  def test_get_columns_super_sub
  #    @statuses.insert(key, :relationships, {
  #      'user_timelines' => {'1' => 'v1'}, 
  #      'mentions_timelines' => {'2' => 'v2'}})
  #    assert_equal ['v1', 'v2'], 
  #      @statuses.get_columns(key, :relationships, 'user_timelines', ['1', key])
  #  end    
  
  def test_count_keys
    @statuses.insert(key + "1", :row, {'body' => 'v1'})
    @statuses.insert(key + "2", :row, {'body' => 'v1'})
    @statuses.insert(key + "3", :row, {'body' => 'v1'})
    assert_equal 3, @statuses.count(:row)  
  end
  
  def test_count_columns
    @statuses.insert(key, :row, {'body' => 'v1', 'user' => 'v2'})
    assert_equal 2, @statuses.count_columns(key, :row)
  end 

  def test_count_super_columns
    @statuses.insert(key, :relationships, {
      'user_timelines' => {'1' => 'v1'}, 
      'mentions_timelines' => {'2' => 'v2'}})
    assert_equal 2, @statuses.count_columns(key, :relationships)
  end 

  def test_count_super_sub_columns
    @statuses.insert(key, :relationships, {'user_timelines' => {'1' => 'v1', key => 'v2'}})
    assert_equal 2, @statuses.count_columns(key, :relationships, 'user_timelines')
  end
  
  private
  
  def key
    caller.first[/`(.*?)'/, 1]
  end
end
