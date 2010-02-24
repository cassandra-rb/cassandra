module Connection
  class Base
    attr_accessor :transport, :server

    def initialize(transport, transport_wrapper, server, timeout)
      @transport = transport
      @transport_wrapper = transport_wrapper
      @server = server
      @timeout = timeout
    end

    def connect!
      raise NotImplementedError
    end

    def close
    end
  end
end
