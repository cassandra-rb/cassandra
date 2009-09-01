
module CassandraThrift #:nodoc: all
  module Cassandra
  
    class SafeClient  
      def initialize(client, transport, buffer)
        @client = client 
        @transport = transport
        @buffer = buffer
      end
      
      def reset_transport
        @transport.close rescue nil
        @transport.open
      end
      
      def method_missing(*args)
        reset_transport unless @buffer
        @client.send(*args)
      rescue IOError, UnavailableException, Thrift::ProtocolException
        reset_transport
        @client.send(*args)
      end
    end
  end
end
