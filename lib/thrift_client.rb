if ENV["ANCIENT_THRIFT"]
  $LOAD_PATH.unshift("/Users/eweaver/p/twitter/rails/vendor/gems/thrift-751142/lib")
  $LOAD_PATH.unshift("/Users/eweaver/p/twitter/rails/vendor/gems/thrift-751142/ext")
  require 'thrift'
else
  require 'rubygems'
  require 'thrift'
end

require 'rubygems'
require 'thrift_client/thrift'
require 'thrift_client/connection'


class AbstractThriftClient

  DEFAULTS = {
    :protocol => Thrift::BinaryProtocol,
    :protocol_extra_params => [],
    :transport => Thrift::Socket,
    :transport_wrapper => Thrift::FramedTransport,
    # FIXME everything below here should be in the modules that care
    # about it
    :randomize_server_list => true,
    :raise => true, # FIXME hrm.
    :retries => nil,
    :server_retry_period => 1,
    :server_max_requests => nil,
    :timeout => 1,
    :timeout_overrides => {},
    :defaults => {}
  }.freeze

  # FIXME Document client_methods
  # FIXME disconnect!(false) in RetryingThriftClient
  # FIXME test_buffered_transport_timeout,
  # test_framed_transport_timeout and test_buffered_transport_timeout
  # is commented/written wrong

  attr_reader :client, :client_class, :current_server, :server_list, :options, :client_methods

=begin rdoc
Create a new ThriftClient instance. Accepts an internal Thrift client class (such as CassandraRb::Client), a list of servers with ports, and optional parameters.

Valid optional parameters are:

<tt>:protocol</tt>:: Which Thrift protocol to use. Defaults to <tt>Thrift::BinaryProtocol</tt>.
<tt>:protocol_extra_params</tt>:: An array of additional parameters to pass to the protocol initialization call. Defaults to <tt>[]</tt>.
<tt>:transport</tt>:: Which Thrift transport to use. Defaults to <tt>Thrift::FramedTransport</tt>.
<tt>:randomize_server_list</tt>:: Whether to connect to the servers randomly, instead of in order. Defaults to <tt>true</tt>.
<tt>:exception_classes</tt>:: Which exceptions to catch and retry when sending a request. Defaults to <tt>[IOError, Thrift::Exception, Thrift::ProtocolException, Thrift::ApplicationException, Thrift::TransportException, NoServersAvailable]</tt>
<tt>:raise</tt>:: Whether to reraise errors if no responsive servers are found. Defaults to <tt>true</tt>.
<tt>:retries</tt>:: How many times to retry a request. Defaults to the number of servers defined.
<tt>:server_retry_period</tt>:: How many seconds to wait before trying to reconnect after marking all servers as down. Defaults to <tt>1</tt>. Set to <tt>nil</tt> to retry endlessly.
<tt>:server_max_requests</tt>:: How many requests to perform before moving on to the next server in the pool, regardless of error status. Defaults to <tt>nil</tt> (no limit).
<tt>:timeout</tt>:: Specify the default timeout in seconds. Defaults to <tt>1</tt>.
<tt>:timeout_overrides</tt>:: Specify additional timeouts on a per-method basis, in seconds. Only works with <tt>Thrift::BufferedTransport</tt>.
<tt>:defaults</tt>:: Specify default values to return on a per-method basis, if <tt>:raise</tt> is set to false.

=end rdoc

  def initialize(client_class, servers, options = {})
    @options = DEFAULTS.merge(options)
    @client_class = client_class
    @server_list = Array(servers)
    @current_server = @server_list.first

    @client_methods = []
    @client_class.instance_methods.each do |method_name|
      if method_name =~ /^recv_(.*)$/
        instance_eval("def #{$1}(*args); proxy(:'#{$1}', *args); end")
        @client_methods << $1
      end
    end
  end

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

  def proxy(method_name, *args)
    connect! unless @client
    @client.send(method_name, *args)
  end
end

class ThriftClient < AbstractThriftClient
  # FIXME for backwards compatibility only. If defined in
  # RetryingThriftClient instead, causes the test suite to break.
  class NoServersAvailable < StandardError; end
end

module RetryingThriftClient
  DISCONNECT_ERRORS = [
                       IOError,
                       Thrift::Exception,
                       Thrift::ProtocolException,
                       Thrift::ApplicationException,
                       Thrift::TransportException,
                       ThriftClient::NoServersAvailable
                      ]

  def initialize(client_class, servers, options = {})
    super
    @retries = options[:retries] || @server_list.size
    @options[:exception_classes] ||= DISCONNECT_ERRORS

    @request_count = 0
    @max_requests = @options[:server_max_requests]
    @retry_period = @options[:server_retry_period]
    rebuild_live_server_list!

  end

  # Force the client to connect to the server.
  def connect!
    @current_server = next_server
    super
  rescue Thrift::TransportException, Errno::ECONNREFUSED
    retry
  end

  # Force the client to disconnect from the server.
  def disconnect!(keep = true) # FIXME fucked on submodule
    # Keep live servers in the list if we have a retry period. Otherwise,
    # always eject, because we will always re-add them.
    if keep and @retry_period and @current_server
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

  # FIXME we override the original proxy completely just to stick the
  # @request_count addition in there. Which is lame
  def proxy(method_name, *args)
    disconnect! if @max_requests and @request_count >= @max_requests
    connect! unless @client

    @request_count += 1
    @client.send(method_name, *args)

  # FIXME Why is NoServersAvailable here and in :exception_classes?
  rescue ThriftClient::NoServersAvailable => e
    handle_exception(e, method_name, args)
  rescue *@options[:exception_classes] => e
    disconnect!(false)
    tries ||= @retries
    tries -= 1
    tries == 0 ? handle_exception(e, method_name, args) : retry
  rescue Exception => e
    handle_exception(e, method_name, args)
  end

  def handle_exception(e, method_name, args=nil)
    raise e if @options[:raise]
    @options[:defaults][method_name.to_sym]
  end
end

module TimingOutThriftClient
  def connect!
    super
    set_method_timeouts!
  end

  def has_timeouts?
    @has_timeouts ||= has_timeouts!
  end

  private
  def set_method_timeouts!
    return unless has_timeouts?
    @client_methods.each do |method_name|
      @client.timeout = @options[:timeout_overrides][method_name.to_sym] || @options[:timeout]
    end
  end

  def has_timeouts!
    transport_can_timeout? if @options[:timeout_overrides].any?
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

class ThriftClient < AbstractThriftClient
  include RetryingThriftClient
  include TimingOutThriftClient
end
