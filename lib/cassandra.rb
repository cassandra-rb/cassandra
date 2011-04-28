require 'rubygems'
gem 'thrift', '~> 0.5.0'
require 'thrift'
gem 'thrift_client', '~> 0.6.0'
require 'thrift_client'
gem 'simple_uuid' , '~> 0.1.0'
require 'simple_uuid'

require 'json' unless defined?(JSON)

here = File.expand_path(File.dirname(__FILE__))

class Cassandra ; end
unless Cassandra.respond_to?(:VERSION)
  require "#{here}/cassandra/0.7"
end

$LOAD_PATH << "#{here}/../vendor/#{Cassandra.VERSION}/gen-rb"
require "#{here}/../vendor/#{Cassandra.VERSION}/gen-rb/cassandra"

$LOAD_PATH << "#{here}"

require 'cassandra/helpers'
require 'cassandra/array'
require 'cassandra/time'
require 'cassandra/comparable'
require 'cassandra/long'
require 'cassandra/ordered_hash'
require 'cassandra/columns'
require "cassandra/#{Cassandra.VERSION}/columns"
require "cassandra/#{Cassandra.VERSION}/protocol"
require "cassandra/cassandra"
require "cassandra/#{Cassandra.VERSION}/cassandra"
unless Cassandra.VERSION.eql?("0.6")
  require "cassandra/#{Cassandra.VERSION}/column_family"
  require "cassandra/#{Cassandra.VERSION}/keyspace"
end
require 'cassandra/constants'
require 'cassandra/debug' if ENV['DEBUG']
