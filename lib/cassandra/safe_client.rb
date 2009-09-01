
module CassandraThrift #:nodoc: all
  module Cassandra
  
    class SafeClient  
      def initialize(client, transport, reset = false)
        @client = client 
        @transport = transport
        @reset = reset
      end
      
      def reset_transport
        @transport.close rescue nil
        @transport.open
      end
      
      def method_missing(*args)
        reset_transport if @reset
        @client.send(*args)
      rescue IOError, UnavailableException, Thrift::ProtocolException, Thrift::ApplicationException, Thrift::TransportException
        reset_transport
        @client.send(*args)
      end
    end
  end
end
