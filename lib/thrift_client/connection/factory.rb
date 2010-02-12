module Connection
  class Factory
    def self.create(thrift_client_instance)
      case thrift_client_instance.options[:transport].to_s
      when "Thrift::HTTPClientTransport"
        Connection::HTTP.new(thrift_client_instance, :handles_error => Errno::ECONNREFUSED)
      else
        Connection::Socket.new(thrift_client_instance, :handles_error => Thrift::TransportException)
      end
    end
  end
end