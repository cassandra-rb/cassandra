require 'rubygems'
gem 'thrift_client', '~> 0.7'
require 'thrift_client'
gem 'simple_uuid' , '~> 0.3'
require 'simple_uuid'

require 'json' unless defined?(JSON)

here = File.expand_path(File.dirname(__FILE__))

class CassandraOld ; end
unless CassandraOld.respond_to?(:VERSION)
  require "#{here}/cassandra_old/0.8"
end

$LOAD_PATH << "#{here}/../vendor/#{CassandraOld.VERSION}/gen-rb"
require "#{here}/../vendor/#{CassandraOld.VERSION}/gen-rb/cassandra"

$LOAD_PATH << "#{here}"

require 'cassandra_old/helpers'
require 'cassandra_old/array'
require 'cassandra_old/time'
require 'cassandra_old/comparable'
require 'cassandra_old/long'
require 'cassandra_old/composite'
require 'cassandra_old/dynamic_composite'
require 'cassandra_old/ordered_hash'
require 'cassandra_old/columns'
require 'cassandra_old/protocol'
require 'cassandra_old/batch'
require "cassandra_old/#{CassandraOld.VERSION}/columns"
require "cassandra_old/#{CassandraOld.VERSION}/protocol"
require "cassandra_old/cassandra"
require "cassandra_old/#{CassandraOld.VERSION}/cassandra"
unless CassandraOld.VERSION.eql?("0.6")
  require "cassandra_old/column_family"
  require "cassandra_old/keyspace"
end
require 'cassandra_old/constants'
require 'cassandra_old/debug' if ENV['DEBUG']

begin
  require "cassandra_native"
rescue LoadError
  puts "Unable to load cassandra_native extension. Defaulting to pure Ruby libraries."
end
