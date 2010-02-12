module Connection
  class Base
    attr_accessor :transport, :server

    def initialize(transport, transport_wrapper, server, timeout, error_hash)
      @transport = transport
      @transport_wrapper = transport_wrapper
      @server = server
      @timeout = timeout
      @error_type = error_hash[:handles_error]
    end

    def connect!
      raise NotImplementedError
    end

    def close
    end

  private

    def handle_error
    end
  end
end