require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'cassandra_test'
require 'cassandra/mock'

class CassandraMockTest < CassandraTest
  include Cassandra::Constants

  def setup
    @twitter = Cassandra::Mock.new('Twitter')
    @twitter.clear_keyspace!

    @blogs = Cassandra::Mock.new('Multiblog')
    @blogs.clear_keyspace!

    @blogs_long = Cassandra::Mock.new('MultiblogLong')
    @blogs_long.clear_keyspace!

    @uuids = (0..6).map {|i| UUID.new(Time.at(2**(24+i))) }
    @longs = (0..6).map {|i| Long.new(Time.at(2**(24+i))) }
  end

  def test_setup
    assert @twitter
    assert @blogs
    assert @blogs_long
  end
end
