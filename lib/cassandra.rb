
require 'rubygems'
require 'thrift_client'

HERE = File.expand_path(File.dirname(__FILE__))

$LOAD_PATH << "#{HERE}/../vendor/gen-rb"
require "#{HERE}/../vendor/gen-rb/cassandra"

$LOAD_PATH << "#{HERE}"
require 'cassandra/array'
require 'cassandra/time'
require 'cassandra/comparable'
require 'cassandra/uuid'
require 'cassandra/long'
require 'cassandra/ordered_hash'
require 'cassandra/columns'
require 'cassandra/protocol'
require 'cassandra/cassandra'
require 'cassandra/constants'
require 'cassandra/debug' if ENV['DEBUG']
