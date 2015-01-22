require 'rubygems'
gem 'thrift_client', '~> 0.7'
require 'thrift_client'
gem 'simple_uuid' , '~> 0.3'
require 'simple_uuid'

require 'json' unless defined?(JSON)

here = File.expand_path(File.dirname(__FILE__))

class TwitterCassandra ; end
unless TwitterCassandra.respond_to?(:VERSION)
  require "#{here}/cassandra/0.8"
end

$LOAD_PATH << "#{here}/../vendor/#{TwitterCassandra.VERSION}/gen-rb"
require "#{here}/../vendor/#{TwitterCassandra.VERSION}/gen-rb/cassandra"

$LOAD_PATH << "#{here}"

require 'twitter_cassandra/helpers'
require 'twitter_cassandra/array'
require 'twitter_cassandra/time'
require 'twitter_cassandra/comparable'
require 'twitter_cassandra/long'
require 'twitter_cassandra/composite'
require 'twitter_cassandra/dynamic_composite'
require 'twitter_cassandra/ordered_hash'
require 'twitter_cassandra/columns'
require 'twitter_cassandra/protocol'
require 'twitter_cassandra/batch'
require "twitter_cassandra/#{TwitterCassandra.VERSION}/columns"
require "twitter_cassandra/#{TwitterCassandra.VERSION}/protocol"
require "twitter_cassandra/cassandra"
require "twitter_cassandra/#{TwitterCassandra.VERSION}/cassandra"
unless TwitterCassandra.VERSION.eql?("0.6")
  require "twitter_cassandra/column_family"
  require "twitter_cassandra/keyspace"
end
require 'twitter_cassandra/constants'
require 'twitter_cassandra/debug' if ENV['DEBUG']

begin
  require "cassandra_native"
rescue LoadError
  puts "Unable to load cassandra_native extension. Defaulting to pure Ruby libraries."
end
