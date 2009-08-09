require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra"

begin; require 'ruby-debug'; rescue LoadError; end


begin
  @test_client = Cassandra.new('Twitter', '127.0.0.1')
rescue Thrift::TransportException => e
  #TODO Server autorun
  raise "Make sure that cassandra's server is runing. You can start it by running rake cassandra"  if e.message =~ /Could not connect/
end
