module Connection
  class Factory
    def self.create(transport, transport_wrapper, server, timeout)
      if transport == Thrift::HTTPClientTransport
        Connection::HTTP.new(transport, transport_wrapper, server, timeout)
      else
        Connection::Socket.new(transport, transport_wrapper, server, timeout)
      end
    end
  end
end
