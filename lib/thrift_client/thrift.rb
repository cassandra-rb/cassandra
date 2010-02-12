module Thrift
  class BufferedTransport
    def timeout=(timeout)
      @transport.timeout = timeout
    end

    def timeout
      @transport.timeout
    end
  end

  module Client
    def timeout=(timeout)
      @iprot.trans.timeout = timeout
    end

    def timeout
      @iprot.trans.timeout
    end
  end
end
