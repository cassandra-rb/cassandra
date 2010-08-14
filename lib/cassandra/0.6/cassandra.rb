class Cassandra
  def self.DEFAULT_TRANSPORT_WRAPPER
    Thrift::BufferedTransport
  end
  
  ## Delete

  # Remove all rows in the column family you request. Supports options
  # <tt>:consistency</tt> and <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_column_family!(column_family, options = {})
    each_key(column_family) do |key|
      remove(column_family, key, options)
    end
  end

  # Remove all rows in the keyspace. Supports options <tt>:consistency</tt> and
  # <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_keyspace!(options = {})
    schema.keys.each { |column_family| clear_column_family!(column_family, options) }
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
    check_keyspace
  end

  def check_keyspace
    unless (keyspaces = client.get_string_list_property("keyspaces")).include?(@keyspace)
      raise AccessError, "Keyspace #{@keyspace.inspect} not found. Available: #{keyspaces.inspect}"
    end
  end

  def all_nodes
    if @auto_discover_nodes
      ips = ::JSON.parse(new_client.get_string_property('token map')).values
      port = @servers.first.split(':').last
      ips.map{|ip| "#{ip}:#{port}" }
    else
      @servers
    end
  end

end