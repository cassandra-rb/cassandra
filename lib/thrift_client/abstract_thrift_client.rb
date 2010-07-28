class AbstractThriftClient

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :protocol_extra_params => [],
    :transport => Thrift::Socket,
    :transport_wrapper => Thrift::FramedTransport,
    :raise => true,
    :defaults => {}
  }.freeze

  attr_reader :client, :client_class, :current_server, :server_list, :options, :client_methods

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @current_server = @server_list.first

    @client_methods = []
    @client_class.instance_methods.each do |method_name|
      if method_name =~ /^recv_(.*)$/
        instance_eval("def #{$1}(*args); handled_proxy(:'#{$1}', *args); end", __FILE__, __LINE__)
        @client_methods << $1
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
    @connection = Connection::Factory.create(@options[:transport], @options[:transport_wrapper], @current_server, @options[:timeout])
    @connection.connect!
    @client = @client_class.new(@options[:protocol].new(@connection.transport, *@options[:protocol_extra_params]))
  rescue Thrift::TransportException, Errno::ECONNREFUSED => e
    @transport.close rescue nil
    raise e
  end

  def disconnect!
    @connection.close rescue nil
    @client = nil
    @current_server = nil
  end

  private
  def handled_proxy(method_name, *args)
    proxy(method_name, *args)
  rescue Exception => e
    handle_exception(e, method_name, args)
  end

  def proxy(method_name, *args)
    connect! unless @client
    send_rpc(method_name, *args)
  end

  def send_rpc(method_name, *args)
    @client.send(method_name, *args)
  end

  def disconnect_on_error!
    @connection.close rescue nil
    @client = nil
    @current_server = nil
  end

  def handle_exception(e, method_name, args=nil)
    raise e if @options[:raise]
    @options[:defaults][method_name.to_sym]
  end

  module RetryingThriftClient
    DISCONNECT_ERRORS = [
                         IOError,
                         Thrift::Exception,
                         Thrift::ProtocolException,
                         Thrift::ApplicationException,
                         Thrift::TransportException
                        ].freeze

    RETRYING_DEFAULTS = {
      :exception_classes => DISCONNECT_ERRORS,
      :randomize_server_list => true,
      :retries => 0,
      :server_retry_period => 1,
      :server_max_requests => nil,
      :retry_overrides => {}
    }.freeze

    def initialize(client_class, servers, options = {})
      super
      @options = RETRYING_DEFAULTS.merge(@options) # @options is set by super
      @retries = @options[:retries]
      @request_count = 0
      @max_requests = @options[:server_max_requests]
      @retry_period = @options[:server_retry_period]
      rebuild_live_server_list!
    end

    def connect!
      @current_server = next_server
      super
    rescue Thrift::TransportException, Errno::ECONNREFUSED
      retry
    end

    def disconnect!
      # Keep live servers in the list if we have a retry period. Otherwise,
      # always eject, because we will always re-add them.
      if @retry_period && @current_server
        @live_server_list.unshift(@current_server)
      end

      super()
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
      super
    end

    def proxy(method_name, *args)
      super
    rescue *@options[:exception_classes] => e
      disconnect_on_error!
      tries ||= (@options[:retry_overrides][method_name.to_sym] || @retries) + 1
      tries -= 1
      tries > 0 ? retry : raise
    end

    def send_rpc(method_name, *args)
      @request_count += 1
      super
    end

    def disconnect_on_max!
      @live_server_list.push(@current_server)
      disconnect_on_error!
    end

    def disconnect_on_error!
      super
      @request_count = 0
    end

  end

  module TimingOutThriftClient
    TIMINGOUT_DEFAULTS = {
      :timeout => 1,
      :timeout_overrides => {}
    }.freeze

    def initialize(client_class, servers, options = {})
      super
      @options = TIMINGOUT_DEFAULTS.merge(@options)
    end

    def connect!
      super
      set_method_timeouts!
    end

    private
    def set_method_timeouts!
      return unless has_timeouts?
      @client_methods.each do |method_name|
        @client.timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
      end
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
end
