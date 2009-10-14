
require 'rubygems'
require 'thrift'

class ThriftClient  
  DEFAULTS = { 
    :protocol => Thrift::BinaryProtocol,
    :transport => Thrift::FramedTransport,
    :timeout => 0.2, 
    :server_retry_delay => 15, 
    :randomize_server_list => true
  }
  
  attr_reader :server_list, :options

  def initialize(client_class, port, servers, options = {})
    @client_class = client_class        
    @server_list = Array(servers)
    @server_list.sort_by! { rand } if @options[:randomize_server_list]
    @port = port
    @options = DEFAULTS.merge(options)
    @last_retry = Time.now.to_i

    @attempts = 0
    @live_server_list = []    
    reconnect!    
  end
  
  def reconnect!
    @transport.close rescue nil
    @transport = @options[:transport].new(Thrift::Socket.new(next_server, @port))
    @transport.open
    @client = @client_class.new(@options[:protocol].new(@transport, false))
  end
  
  def method_missing(*args)
    attempts ||= 0
    @client.send(*args)    
  rescue IOError, UnavailableException, Thrift::ProtocolException, Thrift::ApplicationException, Thrift::TransportException => e
    raise if attempts > @server_list.size
    reconnect!
    attempts += 1
    retry
  end

  def next_server
    @live_server_list = @server_list.dup if @live_server_list.empty?
    @live_server_list.pop
  end
end
