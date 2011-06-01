class Cassandra
  def self.DEFAULT_TRANSPORT_WRAPPER
    Thrift::BufferedTransport
  end

  ##
  # Issues a login attempt using the username and password specified.
  #
  # * username
  # * password
  #
  def login!(username, password)
    @auth_request = CassandraThrift::AuthenticationRequest.new
    @auth_request.credentials = {'username' => username, 'password' => password}
    client.login(@keyspace, @auth_request)
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema(false).map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @servers=#{servers.inspect}>"
  end

  ##
  # Returns an array of available keyspaces.
  #
  def keyspaces
    @keyspaces ||= client.describe_keyspaces()
  end

  ##
  # Remove all rows in the column family you request.
  #
  # * column_family
  # * options
  #   * consitency
  #   * timestamp
  #
  def clear_column_family!(column_family, options = {})
    each_key(column_family) do |key|
      remove(column_family, key, options)
    end
  end
  alias truncate! clear_column_family!

  # Remove all rows in the keyspace. Supports options <tt>:consistency</tt> and
  # <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_keyspace!(options = {})
    schema.keys.each { |column_family| clear_column_family!(column_family, options) }
  end

  # Open a batch operation and yield self. Inserts and deletes will be queued
  # until the block closes, and then sent atomically to the server.  Supports
  # the <tt>:consistency</tt> option, which overrides the consistency set in
  # the individual commands.
  def batch(options = {})
    _, _, _, options = 
      extract_and_validate_params(schema.keys.first, "", [options], WRITE_DEFAULTS)

    @batch = []
    yield(self)
    compacted_map,seen_clevels = compact_mutations!
    clevel = if options[:consistency] != nil # Override any clevel from individual mutations if 
               options[:consistency]
             elsif seen_clevels.length > 1 # Cannot choose which CLevel to use if there are several ones
               raise "Multiple consistency levels used in the batch, and no override...cannot pick one" 
             else # if no consistency override has been provided but all the clevels in the batch are the same: use that one
               seen_clevels.first
             end

    _mutate(compacted_map,clevel)
  ensure
    @batch = nil
  end

  protected

  def schema(load=true)
    if !load && !@schema
      []
    else
      @schema ||= client.describe_keyspace(@keyspace)
    end
  end

  def client
    reconnect! if @client.nil?
    @client
  end

  def reconnect!
    @servers = all_nodes
    @client = new_client
  end

  def all_nodes
    if @auto_discover_nodes
      temp_client = new_client
      begin
        ips = ::JSON.parse(temp_client.get_string_property('token map')).values
        port = @servers.first.split(':').last
        ips.map{|ip| "#{ip}:#{port}" }
      ensure 
        temp_client.disconnect!
      end
    else
      @servers
    end
  end

end
