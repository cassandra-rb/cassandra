
if ENV["ANCIENT_THRIFT"]
  $LOAD_PATH.unshift("/Users/eweaver/p/twitter/rails/vendor/gems/thrift-751142/lib")
  $LOAD_PATH.unshift("/Users/eweaver/p/twitter/rails/vendor/gems/thrift-751142/ext")
  require 'thrift'
else
  require 'rubygems'
  require 'thrift'
end

require 'timeout'
require 'rubygems'
require 'thrift_client/thrift'
require 'net/http'

class ThriftClient
  class NoServersAvailable < StandardError; end
  class GlobalThriftClientTimeout < Timeout::Error; end

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :protocol_extra_params => [],
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
    :server_retry_period => 1,
    :server_max_requests => nil,
    :timeout => 1,
    :timeout_overrides => {},
    :defaults => {}
  }.freeze

  attr_reader :client, :client_class, :current_server, :server_list, :options

=begin rdoc
Create a new ThriftClient instance. Accepts an internal Thrift client class (such as CassandraRb::Client), a list of servers with ports, and optional parameters.

Valid optional parameters are:

<tt>:protocol</tt>:: Which Thrift protocol to use. Defaults to <tt>Thrift::BinaryProtocol</tt>.
<tt>:protocol_extra_params</tt>:: An array of additional parameters to pass to the protocol initialization call. Defaults to <tt>[]</tt>.
<tt>:transport</tt>:: Which Thrift transport to use. Defaults to <tt>Thrift::FramedTransport</tt>.
<tt>:randomize_server_list</tt>:: Whether to connect to the servers randomly, instead of in order. Defaults to <tt>true</tt>.
<tt>:raise</tt>:: Whether to reraise errors if no responsive servers are found. Defaults to <tt>true</tt>.
<tt>:retries</tt>:: How many times to retry a request. Defaults to the number of servers defined.
<tt>:server_retry_period</tt>:: How many seconds to wait before trying to reconnect after marking all servers as down. Defaults to <tt>1</tt>. Set to <tt>nil</tt> to retry endlessly.
<tt>:server_max_requests</tt>:: How many requests to perform before moving on to the next server in the pool, regardless of error status. Defaults to <tt>nil</tt> (no limit).
<tt>:global_timeout</tt>:: Specify the timeout for all connections made. Defaults to being disabled.
<tt>:timeout</tt>:: Specify the default timeout in seconds per connection. Defaults to <tt>1</tt>.
<tt>:timeout_overrides</tt>:: Specify additional timeouts on a per-method basis, in seconds. Only works with <tt>Thrift::BufferedTransport</tt>.
<tt>:defaults</tt>:: Specify default values to return on a per-method basis, if <tt>:raise</tt> is set to false.

=end rdoc

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @retries = options[:retries] || @server_list.size

    if @options[:timeout_overrides].any?
      if @options[:transport].instance_methods.include?("timeout=")
        @set_timeout = true
      else
        warn "ThriftClient: Timeout overrides have no effect with with transport type #{@options[:transport]}"
      end
    end

    @request_count = 0
    @max_requests = @options[:server_max_requests]
    @retry_period = @options[:server_retry_period]
    @global_timeout = @options[:global_timeout] || 0
    rebuild_live_server_list!

    @client_class.instance_methods.each do |method_name|
      if method_name =~ /^recv_(.*)$/
        instance_eval("def #{$1}(*args); proxy(:'#{$1}', *args); end")
      end
    end
  end

  # Force the client to connect to the server.
  # TODO refactor
  # def connect!
  #   case @options[:transport].to_s
  #   when "Thrift::HTTPClientTransport"
  #     connect_with_http
  #   else
  #     connect_with_socket
  #   end
  # end
  
  class ConnectionFactory
    def self.create(thrift_client_instance)
      case thrift_client_instance.options[:transport].to_s
      when "Thrift::HTTPClientTransport"
        ConnectionHTTP.new(thrift_client_instance)
      else
        ConnectionSocket.new(thrift_client_instance)
      end
    end
  end
  
  class ConnectionBase
    attr_accessor :transport, :server
    
    def initialize(thrift_client_instance)
      @thrift_client = thrift_client_instance
    end
    
    def validate_server_format
      raise "not implemented"
    end
    
    def open_transport
      raise "not implemented"
    end
    
    def close
    end
  end
  
  class ConnectionSocket < ConnectionBase
    def open_transport
      @server = @thrift_client.next_server
      validate_server_format
      @transport = @thrift_client.options[:transport].new(
        Thrift::Socket.new(@host, @port.to_i, @thrift_client.options[:timeout]))
      @transport.open
    rescue Thrift::TransportException
      @transport.close rescue nil
      retry
    end
    
    def validate_server_format
      @host, @port = @server.to_s.split(":")
      raise ArgumentError, 'Servers must be in the form "host:port"' unless @host and @port
    end
    
    def close
      @transport.close
    end
  end
  
  class ConnectionHTTP < ConnectionBase
    def validate_server_format
      @uri = URI.parse(@server)
      raise ArgumentError, 'Servers must start with http' unless @uri.scheme =~ /^http/
    end
    
    def open_transport
      @server = @thrift_client.next_server
      validate_server_format
      @transport = Thrift::HTTPClientTransport.new(@server)
      Net::HTTP.get(@uri)
      # TODO http.use_ssl = @url.scheme == "https"
    rescue Errno::ECONNREFUSED
      retry
    end
  end
  
  def connect!
    @connection = ConnectionFactory.create(self)
    @connection.open_transport # @connection.transport
    @current_server = @connection.server
    @client = @client_class.new(@options[:protocol].new(@connection.transport, *@options[:protocol_extra_params]))
  end
  
  # def connect_with_socket
  #   server = next_server
  #   
  #   host, port = server.to_s.split(":")
  #   raise ArgumentError, 'Servers must be in the form "host:port"' unless host and port
  # 
  #   @transport = @options[:transport].new(
  #     Thrift::Socket.new(host, port.to_i, @options[:timeout]))
  #   @transport.open
  #   @current_server = server
  #   @client = @client_class.new(@options[:protocol].new(@transport, *@options[:protocol_extra_params]))
  # rescue Thrift::TransportException
  #   @transport.close rescue nil
  #   retry
  # end
  # 
  # def connect_with_http
  #   server = next_server
  #   
  #   uri = URI.parse(server)
  #   raise ArgumentError, 'Servers must start with http' unless uri.scheme =~ /^http/
  #   
  #   @transport = @options[:transport].new(server)
  #   Net::HTTP.get(uri)
  #   # TODO http.use_ssl = @url.scheme == "https"
  #   @current_server = server
  #   @client = @client_class.new(@options[:protocol].new(@transport, *@options[:protocol_extra_params]))
  # rescue Errno::ECONNREFUSED
  #   retry
  # end

  # Force the client to disconnect from the server.
  def disconnect!(keep = true)
    @connection.close rescue nil

    # Keep live servers in the list if we have a retry period. Otherwise,
    # always eject, because we will always re-add them.
    if keep and @retry_period and @current_server
      @live_server_list.unshift(@current_server)
    end

    @request_count = 0
    @client = nil
    @current_server = nil
  end
  
  def next_server
    if @retry_period
      rebuild_live_server_list! if Time.now > @last_rebuild + @retry_period
      raise NoServersAvailable, "No live servers in #{@server_list.inspect} since #{@last_rebuild.inspect}." if @live_server_list.empty?
    elsif @live_server_list.empty?
      rebuild_live_server_list!
    end
    @live_server_list.pop
  end
  
  def rebuild_live_server_list!
    @last_rebuild = Time.now
    if @options[:randomize_server_list]
      @live_server_list = @server_list.sort_by { rand }
    else
      @live_server_list = @server_list.dup
    end
  end

  private

  def proxy(method_name, *args)
    Timeout.timeout(@global_timeout, GlobalThriftClientTimeout) do
      raw_proxy(method_name, *args)
    end
  rescue GlobalThriftClientTimeout => e
    disconnect!(false)
    raise e
  end

  def raw_proxy(method_name, *args)
    disconnect! if @max_requests and @request_count >= @max_requests
    connect! unless @client

    set_timeout!(method_name) if @set_timeout
    @request_count += 1
    @client.send(method_name, *args)
  rescue NoServersAvailable => e
    handle_exception(e, method_name, args)
  rescue *@options[:exception_classes] => e
    disconnect!(false)
    tries ||= @retries
    tries -= 1
    tries == 0 ? handle_exception(e, method_name, args) : retry
  end

  def set_timeout!(method_name)
    @client.timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
  end

  def handle_exception(e, method_name, args)
    raise e if @options[:raise]
    @options[:defaults][method_name.to_sym]
  end
end
