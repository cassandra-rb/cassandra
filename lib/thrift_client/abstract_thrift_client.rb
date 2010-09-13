class AbstractThriftClient
  DISCONNECT_ERRORS = [
    IOError,
    Thrift::Exception,
    Thrift::ProtocolException,
    Thrift::ApplicationException,
    Thrift::TransportException
  ]

  DEFAULT_WRAPPED_ERRORS = [
    Thrift::ApplicationException,
    Thrift::TransportException,
  ]

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :protocol_extra_params => [],
    :transport => Thrift::Socket,
    :transport_wrapper => Thrift::FramedTransport,
    :raise => true,
    :defaults => {},
    :exception_classes => DISCONNECT_ERRORS,
    :randomize_server_list => true,
    :retries => 0,
    :server_retry_period => 1,
    :server_max_requests => nil,
    :retry_overrides => {},
    :wrapped_exception_classes => DEFAULT_WRAPPED_ERRORS,
    :timeout => 1,
    :timeout_overrides => {}
  }

  attr_reader :client, :client_class, :current_server, :server_list, :options, :client_methods

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @current_server = @server_list.first

    @client_methods = []
    @client_class.instance_methods.each do |method_name|
      if method_name != 'send_message' && method_name =~ /^send_(.*)$/
        instance_eval("def #{$1}(*args); handled_proxy(:'#{$1}', *args); end", __FILE__, __LINE__)
        @client_methods << $1
      end
    end
    @retries = @options[:retries]
    @request_count = 0
    @max_requests = @options[:server_max_requests]
    @retry_period = @options[:server_retry_period]
    @options[:wrapped_exception_classes].each do |exception_klass|
      name = exception_klass.to_s.split('::').last
      klass = begin
        @client_class.const_get(name)
      rescue NameError
        @client_class.const_set(name, Class.new(exception_klass))
      end
    end
    rebuild_live_server_list!
  end

  def inspect
    "<#{self.class}(#{client_class}) @current_server=#{@current_server}>"
  end

  # Force the client to connect to the server. Not necessary to be
  # called as the connection will be made on the first RPC method
  # call.
  def connect!
    @current_server = next_server
    @connection = Connection::Factory.create(@options[:transport], @options[:transport_wrapper], @current_server, @options[:timeout])
    @connection.connect!
    @client = @client_class.new(@options[:protocol].new(@connection.transport, *@options[:protocol_extra_params]))
  rescue Thrift::TransportException, Errno::ECONNREFUSED
    @transport.close rescue nil
    retry
  end

  def disconnect!
    # Keep live servers in the list if we have a retry period. Otherwise,
    # always eject, because we will always re-add them.
    if @retry_period && @current_server
      @live_server_list.unshift(@current_server)
    end

    @connection.close rescue nil
    @client = nil
    @current_server = nil
    @request_count = 0
  end

  private

  def next_server
    if @retry_period
      rebuild_live_server_list! if Time.now > @last_rebuild + @retry_period
      raise ThriftClient::NoServersAvailable, "No live servers in #{@server_list.inspect} since #{@last_rebuild.inspect}." if @live_server_list.empty?
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

  def handled_proxy(method_name, *args)
    disconnect_on_max! if @max_requests and @request_count >= @max_requests
    begin
      proxy(method_name, *args)
    rescue Exception => e
      handle_exception(e, method_name, args)
    end
  end

  def handle_exception(e, method_name, args=nil)
    raise e if @options[:raise]
    @options[:defaults][method_name.to_sym]
  end

  def proxy(method_name, *args)
    connect! unless @client
    post_connect(method_name)
    send_rpc(method_name, *args)
  rescue *@options[:exception_classes] => e
    disconnect_on_error!
    tries ||= (@options[:retry_overrides][method_name.to_sym] || @retries) + 1
    tries -= 1
    if tries > 0 
      retry
    else
      raise_wrapped_error(e)
    end
  end

  def raise_wrapped_error(e)
    if @options[:wrapped_exception_classes].include?(e.class)
      raise @client_class.const_get(e.class.to_s.split('::').last), e.message, e.backtrace
    else
      raise e
    end
  end

  def send_rpc(method_name, *args)
    @request_count += 1
    @client.send(method_name, *args)
  end

  def disconnect_on_max!
    @live_server_list.push(@current_server)
    disconnect_on_error!
  end

  def disconnect_on_error!
    @connection.close rescue nil
    @client = nil
    @current_server = nil
    @request_count = 0
  end

  def post_connect(method_name)
    return unless has_timeouts?
    @client.timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
  end

  def has_timeouts?
    @has_timeouts ||= has_timeouts!
  end

  def has_timeouts!
    @options[:timeout_overrides].any? && transport_can_timeout?
  end

  def transport_can_timeout?
      if (@options[:transport_wrapper] || @options[:transport]).method_defined?(:timeout=)
        true
      else
        warn "ThriftClient: Timeout overrides have no effect with with transport type #{(@options[:transport_wrapper] || @options[:transport])}"
        false
      end
    end
end
