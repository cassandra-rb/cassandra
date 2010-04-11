require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'cassandra_test'
require 'cassandra/mock'

class CassandraMockTest < CassandraTest
  include Cassandra::Constants

  def setup
    storage_xml_path = File.expand_path(File.join(File.dirname(File.dirname(__FILE__)), 'conf', 'storage-conf.xml'))
    @twitter = Cassandra::Mock.new('Twitter', storage_xml_path)
    @twitter.clear_keyspace!

    @blogs = Cassandra::Mock.new('Multiblog', storage_xml_path)
    @blogs.clear_keyspace!

    @blogs_long = Cassandra::Mock.new('MultiblogLong', storage_xml_path)
    @blogs_long.clear_keyspace!

    @uuids = (0..6).map {|i| SimpleUUID::UUID.new(Time.at(2**(24+i))) }
    @longs = (0..6).map {|i| Long.new(Time.at(2**(24+i))) }
  end

  def test_setup
    assert @twitter
    assert @blogs
    assert @blogs_long
  end
  
  def test_schema_for_keyspace
    data = {
      "StatusRelationships"=>{
          "CompareSubcolumnsWith"=>"org.apache.cassandra.db.marshal.TimeUUIDType", 
          "CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type",
          "Type"=>"Super"},
      "StatusAudits"=>{
        "CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type",
        "Type"=>"Standard"}, 
      "Statuses"=>{
        "CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type",
        "Type"=>"Standard"}, 
      "UserRelationships"=>{
        "CompareSubcolumnsWith"=>"org.apache.cassandra.db.marshal.TimeUUIDType", 
        "CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type",
        "Type"=>"Super"}, 
      "UserAudits"=>{
        "CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type", 
        "Type"=>"Standard"},
      "Users"=>{"CompareWith"=>"org.apache.cassandra.db.marshal.UTF8Type", "Type"=>"Standard"},
      "TimelinishThings"=>
        {"CompareWith"=>"org.apache.cassandra.db.marshal.BytesType", "Type"=>"Standard"}
    }
    stuff = @twitter.send(:schema_for_keyspace, 'Twitter')
    data.keys.each do |k|
      assert_equal data[k], stuff[k], k
    end
  end

  def test_sorting_row_keys
    @twitter.insert(:Statuses, 'b', {:text => 'foo'})
    @twitter.insert(:Statuses, 'a', {:text => 'foo'})
    assert_equal ['a'], @twitter.get_range(:Statuses, :count => 1)
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
