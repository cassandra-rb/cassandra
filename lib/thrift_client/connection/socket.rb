module Connection
  class Socket < Base
    def close
      @transport.close
    end

    def connect!
      host, port = parse_server(@server)
      @transport = @transport.new(*[host, port.to_i, @timeout])
      @transport = @transport_wrapper.new(@transport) if @transport_wrapper
      @transport.open
    end

  private

    def parse_server(server)
      host, port = server.to_s.split(":")
      raise ArgumentError, 'Servers must be in the form "host:port"' unless host and port
      [host, port]
    end
  end
end
