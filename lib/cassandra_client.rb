
HERE = File.expand_path(File.dirname(__FILE__))

require "#{HERE}/cassandra_client/client"
require "#{HERE}/cassandra_client/table"
require "#{HERE}/../vendor/ordered_hash"

require 'rubygems'
require 'thrift'

$LOAD_PATH << "#{HERE}/../vendor/gen-rb"
require 'cassandra'
