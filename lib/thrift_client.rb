
require 'rubygems'
require 'thrift'
require 'thrift_client/thrift'

class ThriftClient

  class NoServersAvailable < StandardError; end

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :transport => Thrift::FramedTransport,
    :randomize_server_list => true,
    :exception_classes => [
      IOError,
      Thrift::Exception,
      Thrift::ProtocolException,
      Thrift::ApplicationException,
      Thrift::TransportException,
      NoServersAvailable],
    :raise => true,
    :retries => nil,
    :server_retry_period => nil,
    :timeouts => Hash.new(1),
    :defaults => {}
  }.freeze

  attr_reader :client, :client_class, :server_list, :options

=begin rdoc
Create a new ThriftClient instance. Accepts an internal Thrift client class (such as CassandraRb::Client), a list of servers with ports, and optional parameters.

Valid optional parameters are:

<tt>:protocol</tt>:: Which Thrift protocol to use. Defaults to <tt>Thrift::BinaryProtocol</tt>.
<tt>:transport</tt>:: Which Thrift transport to use. Defaults to <tt>Thrift::FramedTransport</tt>.
<tt>:randomize_server_list</tt>:: Whether to connect to the servers randomly, instead of in order. Defaults to <tt>true</tt>.
<tt>:raise</tt>:: Whether to reraise errors if no responsive servers are found. Defaults to <tt>true</tt>.
<tt>:retries</tt>:: How many times to retry a request. Defaults to the number of servers defined.
<tt>:server_retry_period</tt>:: How long to wait before trying to reconnect after marking all servers as down. Defaults to <tt>nil</tt> (do not wait).
<tt>:timeouts</tt>:: Specify timeouts on a per-method basis. Defaults to 1 second for everything. (Per-method values only work with <tt>Thrift::BufferedTransport</tt>.)
<tt>:defaults</tt>:: Specify defaults to return on a per-method basis, if <tt>:raise</tt> is set to false.

=end rdoc

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @retries = options[:retries] || @server_list.size
    @server_list = @server_list.sort_by { rand } if @options[:randomize_server_list]

    @set_timeout = @options[:transport].instance_methods.include?("timeout=")
    @live_server_list = @server_list.dup
    @last_retry = Time.now    

    @client_class.instance_methods.each do |method_name|
      if method_name =~ /^recv_(.*)$/
        instance_eval("def #{$1}(*args); proxy(:'#{$1}', *args); end")
      end
    end
  end

  # Force the client to connect to the server.
  def connect!
    server = next_server.to_s.split(":")
    raise ArgumentError, 'Servers must be in the form "host:port"' if server.size != 2

    @transport = @options[:transport].new(
      Thrift::Socket.new(server.first, server.last.to_i, @options[:timeouts].default))
    @transport.open
    @client = @client_class.new(@options[:protocol].new(@transport, false))
  end

  # Force the client to disconnect from the server.
  def disconnect!
    @transport.close rescue nil
    @client = nil
  end

  private

  def proxy(method_name, *args)
    connect! unless @client
    set_timeout!(method_name) if @set_timeout
    @client.send(method_name, *args)
  rescue *@options[:exception_classes] => e
    tries ||= @retries
    tries -= 1
    if tries.zero? or e.is_a?(NoServersAvailable)
      handle_exception(e, method_name, args)
    else
      disconnect!
      retry
    end
  end
  
  def set_timeout!(method_name)
    @client.timeout = @options[:timeouts][:method_name.to_sym]
  end
  
  def handle_exception(e, method_name, args)
    raise e if @options[:raise]
    @options[:defaults][method_name.to_sym]
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
