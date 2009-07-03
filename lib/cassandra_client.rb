
require 'zlib'
require 'rubygems'
require 'thrift'
require 'json/ext'
require 'json/add/core'

HERE = File.expand_path(File.dirname(__FILE__))

require "#{HERE}/cassandra_client/client"
require "#{HERE}/cassandra_client/table"
require "#{HERE}/cassandra_client/serialization"
require "#{HERE}/../vendor/ordered_hash"

$LOAD_PATH << "#{HERE}/../vendor/gen-rb"
require 'cassandra'
