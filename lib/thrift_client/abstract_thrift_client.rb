class AbstractThriftClient

  class Server
    attr_reader :connection_string, :marked_down_at

    def initialize(connection_string)
      @connection_string = connection_string
    end
    alias to_s connection_string

    def mark_down!
      @marked_down_at = Time.now
    end
  end

  DISCONNECT_ERRORS = [
    IOError,
    Thrift::Exception,
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
    @options[:server_retry_period] ||= 0
    @client_class = client_class
    @server_list = Array(servers).collect{|s| Server.new(s)}.sort_by { rand }
    @current_server = @server_list.first

    @client_methods = []
    @client_class.instance_methods.each do |method_name|
      if method_name != 'send_message' && method_name =~ /^send_(.*)$/
        instance_eval("def #{$1}(*args); handled_proxy(:'#{$1}', *args); end", __FILE__, __LINE__)
        @client_methods << $1
      end
    end
    @request_count = 0
    @options[:wrapped_exception_classes].each do |exception_klass|
      name = exception_klass.to_s.split('::').last
      begin
        @client_class.const_get(name)
      rescue NameError
        @client_class.const_set(name, Class.new(exception_klass))
      end
    end
  end

  def inspect
    "<#{self.class}(#{client_class}) @current_server=#{@current_server}>"
  end

  # Force the client to connect to the server. Not necessary to be
  # called as the connection will be made on the first RPC method
  # call.
  def connect!
    @current_server = next_live_server
    @connection = Connection::Factory.create(@options[:transport], @options[:transport_wrapper], @current_server.connection_string, @options[:timeout])
    @connection.connect!
    @client = @client_class.new(@options[:protocol].new(@connection.transport, *@options[:protocol_extra_params]))
  end

  def disconnect!
    @connection.close rescue nil #TODO
    @client = nil
    @current_server = nil
    @request_count = 0
  end

  private

  def next_live_server
    @server_index ||= 0
    @server_list.length.times do |i|
      cur = (1 + @server_index + i) % @server_list.length
      if !@server_list[cur].marked_down_at || (@server_list[cur].marked_down_at + @options[:server_retry_period] <= Time.now)
        @server_index = cur
        return @server_list[cur]
      end
    end
    raise ThriftClient::NoServersAvailable, "No live servers in #{@server_list.inspect} since #{@last_rebuild.inspect}."
  end

  def handled_proxy(method_name, *args)
    disconnect_on_max! if @options[:server_max_requests] && @request_count >= @options[:server_max_requests]
    begin
      connect! unless @client
      if has_timeouts?
        @client.timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
      end
      @request_count += 1
      @client.send(method_name, *args)
    rescue *@options[:exception_classes] => e
      disconnect_on_error!
      tries ||= (@options[:retry_overrides][method_name.to_sym] || @options[:retries]) + 1
      tries -= 1
      if tries > 0
        retry
      else
        raise_or_default(e, method_name)
      end
    rescue Exception => e
      raise_or_default(e, method_name)
    end
  end

  def raise_or_default(e, method_name)
    if @options[:raise]
      raise_wrapped_error(e)
    else
      @options[:defaults][method_name.to_sym]
    end
  end

  def raise_wrapped_error(e)
    if @options[:wrapped_exception_classes].include?(e.class)
      raise @client_class.const_get(e.class.to_s.split('::').last), e.message, e.backtrace
    else
      raise e
    end
  end

  def disconnect_on_max!
    disconnect!
  end

  def disconnect_on_error!
    @current_server.mark_down!
    disconnect!
  end

  def has_timeouts?
    @has_timeouts ||= @options[:timeout_overrides].any? && transport_can_timeout?
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
