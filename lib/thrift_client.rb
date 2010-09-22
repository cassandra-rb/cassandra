require 'rubygems'
gem 'thrift', '~>0.2.0'
require 'thrift'

require 'rubygems'
require 'thrift_client/thrift'
require 'thrift_client/connection'
require 'thrift_client/abstract_thrift_client'

class ThriftClient < AbstractThriftClient
  class NoServersAvailable < StandardError; end

=begin rdoc
Create a new ThriftClient instance. Accepts an internal Thrift client class (such as CassandraRb::Client), a list of servers with ports, and optional parameters.

Valid optional parameters are:

<tt>:protocol</tt>:: Which Thrift protocol to use. Defaults to <tt>Thrift::BinaryProtocol</tt>.
<tt>:protocol_extra_params</tt>:: An array of additional parameters to pass to the protocol initialization call. Defaults to <tt>[]</tt>.
<tt>:transport</tt>:: Which Thrift transport to use. Defaults to <tt>Thrift::Socket</tt>.
<tt>:transport_wrapper</tt>:: Which Thrift transport wrapper to use. Defaults to <tt>Thrift::FramedTransport</tt>.
<tt>:exception_classes</tt>:: Which exceptions to catch and retry when sending a request. Defaults to <tt>[IOError, Thrift::Exception, Thrift::ApplicationException, Thrift::TransportException, NoServersAvailable]</tt>
<tt>:raise</tt>:: Whether to reraise errors if no responsive servers are found. Defaults to <tt>true</tt>.
<tt>:retries</tt>:: How many times to retry a request. Defaults to 0.
<tt>:server_retry_period</tt>:: How many seconds to wait before trying to reconnect to a dead server. Defaults to <tt>1</tt>. Set to <tt>nil</tt> to disable.
<tt>:server_max_requests</tt>:: How many requests to perform before moving on to the next server in the pool, regardless of error status. Defaults to <tt>nil</tt> (no limit).
<tt>:timeout</tt>:: Specify the default timeout in seconds. Defaults to <tt>1</tt>.
<tt>:timeout_overrides</tt>:: Specify additional timeouts on a per-method basis, in seconds. Only works with <tt>Thrift::BufferedTransport</tt>.
<tt>:defaults</tt>:: Specify default values to return on a per-method basis, if <tt>:raise</tt> is set to false.

=end rdoc

  def initialize(client_class, servers, options = {})
    super
  end
end
