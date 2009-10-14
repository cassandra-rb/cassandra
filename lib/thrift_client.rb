
require 'rubygems'
require 'thrift'

class ThriftClient

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :transport => Thrift::FramedTransport,
    :socket_timeout => 1,
    :randomize_server_list => true,
    :exception_classes => [
      IOError,
      Thrift::Exception,
      Thrift::ProtocolException,
      Thrift::ApplicationException,
      Thrift::TransportException],
    :raise => true,
    :retries => nil,
    :server_retry_period => nil
  }.freeze

  attr_reader :client, :client_class, :server_list, :options

  class NoServersAvailable < StandardError; end

=begin rdoc
Create a new ThriftClient instance. Accepts an internal Thrift client class (such as CassandraRb::Client), a list of servers with ports, and optional parameters.

Valid optional parameters are:

<tt>:protocol</tt>:: Which Thrift protocol to use. Defaults to <tt>Thrift::BinaryProtocol</tt>.
<tt>:transport</tt>:: Which Thrift transport to use. Defaults to <tt>Thrift::FramedTransport</tt>.
<tt>:socket_timeout</tt>:: Timeout to set on the socket connection. Defaults to 1 second.
<tt>:randomize_server_list</tt>:: Whether to connect to the servers randomly, instead of in order. Defaults to <tt>true</tt>.
<tt>:raise</tt>:: Whether to reraise errors if no responsive servers are found. Defaults to <tt>true</tt>.
<tt>:retries</tt>:: How many times to retry a request. Defaults to the number of servers defined.
<tt>:server_retry_period</tt>:: How long to wait before trying to reconnect after marking all servers as down. Defaults to <tt>nil</tt> (do not wait).

=end rdoc

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @retries = options[:retries] || @server_list.size
    @server_list = @server_list.sort_by { rand } if @options[:randomize_server_list]

    @live_server_list = @server_list.dup
    @last_retry = Time.now
  end

  # Force the client to disconnect from the server.
  def disconnect!
    @transport.close rescue nil
    @client = nil
  end

  private

  def method_missing(*args)
    connect! unless @client
    @client.send(*args)
  rescue *@options[:exception_classes]
    tries ||= @retries
    tries -= 1
    if tries.zero?
      raise if @options[:raise]
    else
      disconnect!
      retry
    end
  rescue NoServersAvailable
    raise if @options[:raise]
  end
  
  def connect!
    server = next_server.to_s.split(":")
    raise ArgumentError, 'Servers must be in the form "host:port"' if server.size != 2
    
    @transport = @options[:transport].new(
      Thrift::Socket.new(server.first, server.last.to_i, @options[:socket_timeout]))
    @transport.open
    @client = @client_class.new(@options[:protocol].new(@transport, false))
  end  

  def next_server
    if @live_server_list.empty?
      if @options[:server_retry_period] and Time.now < @last_retry + @options[:server_retry_period]
        raise NoServersAvailable, "No live servers in #{@server_list.inspect} since #{@last_retry.inspect}."
      end
      @last_retry = Time.now
      @live_server_list = @server_list.dup
    end
    @live_server_list.pop
  end
end
