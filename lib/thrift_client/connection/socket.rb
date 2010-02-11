module Connection
  class Socket < Base
    def close
      @transport.close
    end
  
  private

    def force_connection(server)
      host, port = parse_server(server)
      @transport = @thrift_client.options[:transport].new(
        Thrift::Socket.new(host, port.to_i, @thrift_client.options[:timeout]))
      @transport.open
    end
  
    def handle_error
      @transport.close rescue nil
    end
  
    def parse_server(server)
      host, port = server.to_s.split(":")
      raise ArgumentError, 'Servers must be in the form "host:port"' unless host and port
      [host, port]
    end
  end
end