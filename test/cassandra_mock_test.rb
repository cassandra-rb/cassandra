require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require File.expand_path(File.dirname(__FILE__) + '/cassandra_test')
require 'cassandra/mock'
require 'json'

class CassandraMockTest < CassandraTest
  include Cassandra::Constants

  def setup
    @test_schema = JSON.parse(File.read(File.join(File.expand_path(File.dirname(__FILE__)), '..','conf', CASSANDRA_VERSION, 'schema.json')))
    @twitter = Cassandra::Mock.new('Twitter', @test_schema)
    @twitter.clear_keyspace!

    @blogs = Cassandra::Mock.new('Multiblog', @test_schema)
    @blogs.clear_keyspace!

    @blogs_long = Cassandra::Mock.new('MultiblogLong', @test_schema)
    @blogs_long.clear_keyspace!

    @type_conversions = Cassandra::Mock.new('TypeConversions', @test_schema)
    @type_conversions.clear_keyspace!

    @uuids = (0..6).map {|i| SimpleUUID::UUID.new(Time.at(2**(24+i))) }
    @longs = (0..6).map {|i| Long.new(Time.at(2**(24+i))) }
  end

  def test_setup
    assert @twitter
    assert @blogs
    assert @blogs_long
  end
  
  def test_schema_for_keyspace
    data = @test_schema['Twitter']
    stuff = @twitter.send(:schema_for_keyspace, 'Twitter')
    data.keys.each do |k|
      assert_equal data[k], stuff[k], k
    end
  end

  def test_sorting_row_keys
    @twitter.insert(:Statuses, 'b', {:text => 'foo'})
    @twitter.insert(:Statuses, 'a', {:text => 'foo'})
    assert_equal ['a'], @twitter.get_range(:Statuses, :key_count => 1).keys
  end
  
  def test_get_range_reversed
    data = 3.times.map { |i| ["body-#{i.to_s}", "v"] }
    hash = Cassandra::OrderedHash[data]
    reversed_hash = Cassandra::OrderedHash[data.reverse]
    
    @twitter.insert(:Statuses, "all-keys", hash)
    
    columns = @twitter.get_range(:Statuses, :reversed => true)["all-keys"]
    columns.each do |column|
      assert_equal reversed_hash.shift, column
    end
  end
  
  def test_get_range_reversed_slice
    data = 4.times.map { |i| ["body-#{i.to_s}", "v"] }
    hash = Cassandra::OrderedHash[data]
    sliced_hash = Cassandra::OrderedHash[data.reverse[1..-1]]
    
    @twitter.insert(:Statuses, "all-keys", hash)
    
    columns = @twitter.get_range(
      :Statuses,
      :start => sliced_hash.keys.first,
      :reversed => true
    )["all-keys"]
    
    columns.each do |column|
      assert_equal sliced_hash.shift, column
    end
  end
  
  def test_get_range_count
    data = 3.times.map { |i| ["body-#{i.to_s}", "v"] }
    hash = Cassandra::OrderedHash[data]
    
    @twitter.insert(:Statuses, "all-keys", hash)
    
    columns = @twitter.get_range(:Statuses, :count => 2)["all-keys"]
    assert_equal 2, columns.count
  end

  def test_inserting_array_for_indices
    @twitter.insert(:TimelinishThings, 'a', ['1','2'])
    row = @twitter.get(:TimelinishThings, 'a')
    assert_equal({'1' => nil, '2' => nil}, row)

    assert_raises(ArgumentError) {
      @twitter.insert(:UserRelationships, 'a', ['u1','u2'])
    }
  end
end
