
require 'test/unit'
require "#{File.expand_path(File.dirname(__FILE__))}/../lib/cassandra"
begin; require 'ruby-debug'; rescue LoadError; end

begin
  @test_client = Cassandra.new('Twitter', '127.0.0.1')
rescue Thrift::TransportException => e
  #FIXME Make server automatically start if not running
  if e.message =~ /Could not connect/
    puts "*** Please start the Cassandra server by running 'rake cassandra'. ***" 
    exit 1
  end
end
