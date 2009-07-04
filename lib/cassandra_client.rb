
require 'zlib'
require 'rubygems'
require 'thrift'

HERE = File.expand_path(File.dirname(__FILE__))

require "#{HERE}/cassandra_client/client"
require "#{HERE}/cassandra_client/table"
require "#{HERE}/cassandra_client/serialization"
require "#{HERE}/cassandra_client/ordered_hash"

$LOAD_PATH << "#{HERE}/../vendor/gen-rb"
require 'cassandra'
