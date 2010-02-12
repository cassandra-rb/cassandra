module Connection
  class Factory
    def self.create(transport, transport_wrapper, server, timeout)
      if transport == Thrift::HTTPClientTransport
        Connection::HTTP.new(transport, transport_wrapper, server, timeout, :handles_error => Errno::ECONNREFUSED)
      else
        Connection::Socket.new(transport, transport_wrapper, server, timeout, :handles_error => Thrift::TransportException)
      end
    end
  end
end