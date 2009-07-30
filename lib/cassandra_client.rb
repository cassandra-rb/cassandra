
require 'zlib'
require 'rubygems'
require 'thrift'

HERE = File.expand_path(File.dirname(__FILE__))

$LOAD_PATH << "#{HERE}/../vendor/gen-rb"
require 'cassandra'

require "#{HERE}/cassandra_client/array"
require "#{HERE}/cassandra_client/time"
require "#{HERE}/cassandra_client/comparable"
require "#{HERE}/cassandra_client/uuid"
require "#{HERE}/cassandra_client/long"
require "#{HERE}/cassandra_client/safe_client"
require "#{HERE}/cassandra_client/serialization"
require "#{HERE}/cassandra_client/ordered_hash"
require "#{HERE}/cassandra_client/constants"
require "#{HERE}/cassandra_client/helper"
require "#{HERE}/cassandra_client/cassandra_client"
