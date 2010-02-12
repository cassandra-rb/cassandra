module Connection
  class Base
    attr_accessor :transport, :server

    def initialize(thrift_client_instance, error_hash)
      @thrift_client = thrift_client_instance
      @error_type = error_hash[:handles_error]
    end

    def connect!
      @server = @thrift_client.next_server
      force_connection(@server)
    rescue @error_type
      handle_error
      retry
    end

    def close
    end

  private

    def force_connection(server)
      raise NotImplementedError
    end

    def handle_error
    end
  end
end