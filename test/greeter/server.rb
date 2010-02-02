module Greeter
  class Handler
    def greeting(name)
      "hello there #{name}!"
    end
  end
  
  class Server
    def initialize(port)
      @port = port
      handler = Greeter::Handler.new
      processor = Greeter::Processor.new(handler)
      transport = Thrift::ServerSocket.new("127.0.0.1", port)
      transportFactory = Thrift::FramedTransportFactory.new()
      @server = Thrift::SimpleServer.new(processor, transport, transportFactory)
    end
    
    def serve
      @server.serve()
    end
  end
end