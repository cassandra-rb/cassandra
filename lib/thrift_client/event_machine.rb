raise RuntimeError, "The eventmachine transport requires Ruby 1.9.x" if RUBY_VERSION < '1.9.0'

require 'eventmachine'
require 'fiber'

# EventMachine-ready Thrift connection
# Should not be used with a transport wrapper since it already performs buffering in Ruby.
module Thrift
  class EventMachineTransport < BaseTransport
    def initialize(host, port=9090, timeout=5)
      @host, @port, @timeout = host, port, timeout
      @connection = nil
    end

    def open?
      @connection && @connection.connected?
    end

    def open
      @connection = EventMachineConnection.connect(@host, @port, @timeout)
    end

    def close
      @connection.close
    end

    def read(sz)
      @connection.blocking_read(sz)
    end

    def write(buf)
      @connection.send_data(buf)
    end
  end

  module EventMachineConnection
    GARBAGE_BUFFER_SIZE = 4096 # 4kB

    include EM::Deferrable

    def self.connect(host='localhost', port=9090, timeout=5, &block)
      EM.connect(host, port, self, host, port) do |conn|
        conn.pending_connect_timeout = timeout
      end
    end

    def trap
      begin
        yield
      rescue Exception => ex
        puts ex.message
        puts ex.backtrace.join("\n")
      end
    end

    def initialize(host, port=9090)
      @host, @port = host, port
      @index = 0
      @reconnecting = false
      @connected = false
      @buf = ''
    end

    def close
      trap do
        @connected = false
        close_connection(true)
      end
    end

    def blocking_read(size)
      trap do
        if can_read?(size)
          yank(size)
        else
          raise ArgumentError, "Unexpected state" if @size or @callback

          fiber = Fiber.current
          @size = size
          @callback = proc { |data|
            fiber.resume(data)
          }
          Fiber.yield
        end
      end
    end

    def receive_data(data)
      trap do
        (@buf) << data

        if @callback and can_read?(@size)
          callback = @callback
          data = yank(@size)
          @callback = @size = nil
          callback.call(data)
        end
      end
    end

    def connected?
      @connected
    end

    def connection_completed
      @reconnecting = false
      @connected = true
      succeed
    end

    def unbind
      # If we disconnect, try to reconnect
      if @connected or !@reconnecting
        EM.add_timer(1) { 
          # XXX Connect timeout?
          reconnect @host, @port 
        }
        @connected = false
        @reconnecting = true
      end
    end

    def can_read?(size)
      @buf.size >= @index + size
    end

    private

    def yank(len)      
      data = @buf.slice(@index, len)
      @index += len
      @index = @buf.size if @index > @buf.size
      if @index >= GARBAGE_BUFFER_SIZE
        @buf = @buf.slice(@index..-1)
        @index = 0
      end
      data
    end

  end
end