class Cassandra
  def self.DEFAULT_TRANSPORT_WRAPPER
    Thrift::FramedTransport
  end

  def login!(username, password)
    @auth_request = CassandraThrift::AuthenticationRequest.new
    @auth_request.credentials = {'username' => username, 'password' => password}
    client.login(@auth_request)
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      Array(schema(false).cf_defs).map {|cfdef| ":#{cfdef.name} => #{cfdef.column_type}"}.join(', ')
    }}, @servers=#{servers.inspect}>"
  end

  def keyspace=(ks)
    client.set_keyspace(ks) if check_keyspace(ks)
    @schema = nil; @keyspace = ks
  end

  def keyspaces
    client.describe_keyspaces.to_a.collect {|ksdef| ksdef.name }
  end

  def schema(load=true)
    if !load && !@schema
      Cassandra::Keyspace.new
    else
      @schema ||= client.describe_keyspace(@keyspace)
    end
  end

  def schema_agreement?
    client.describe_schema_versions().length == 1
  end

  def version
    client.describe_version()
  end

  def cluster_name
    @cluster_name ||= client.describe_cluster_name()
  end

  def ring
    client.describe_ring(@keyspace)
  end

  def partitioner
    client.describe_partitioner()
  end

  ## Delete

  # Remove all rows in the column family you request.
  def truncate!(column_family)
    #each_key(column_family) do |key|
    #  remove(column_family, key, options)
    #end
    client.truncate(column_family)
  end

  # Remove all rows in the keyspace.
  def clear_keyspace!
    schema.cf_defs.each { |cfdef| truncate!(cfdef.name) }
  end

### Read

  def add_column_family(cf_def)
    begin
      res = client.system_add_column_family(cf_def)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  def drop_column_family(cf_name)
    begin
      res = client.system_drop_column_family(cf_name)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  def rename_column_family(old_name, new_name)
    begin
      res = client.system_rename_column_family(old_name, new_name)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  def add_keyspace(ks_def)
    begin
      res = client.system_add_keyspace(ks_def)
    rescue CassandraThrift::TimedOutException => toe
      puts "Timed out: #{toe.inspect}"
    rescue Thrift::TransportException => te
      puts "Timed out: #{te.inspect}"
    end
    @keyspaces = nil
    res
  end

  def drop_keyspace(ks_name)
    begin
      res = client.system_drop_keyspace(ks_name)
    rescue CassandraThrift::TimedOutException => toe
      puts "Timed out: #{toe.inspect}"
    rescue Thrift::TransportException => te
      puts "Timed out: #{te.inspect}"
    end
    keyspace = "system" if ks_name.eql?(@keyspace)
    @keyspaces = nil
    res
  end

  def rename_keyspace(old_name, new_name)
    begin
      res = client.system_rename_keyspace(old_name, new_name)
    rescue CassandraThrift::TimedOutException => toe
      puts "Timed out: #{toe.inspect}"
    rescue Thrift::TransportException => te
      puts "Timed out: #{te.inspect}"
    end
    keyspace = new_name if old_name.eql?(@keyspace)
    @keyspaces = nil
    res
  end

  # Open a batch operation and yield self. Inserts and deletes will be queued
  # until the block closes, and then sent atomically to the server.  Supports
  # the <tt>:consistency</tt> option, which overrides the consistency set in
  # the individual commands.
  def batch(options = {})
    _, _, _, options =
      extract_and_validate_params(schema.cf_defs.first.name, "", [options], WRITE_DEFAULTS)

    @batch = []
    yield(self)
    compact_mutations!

    @batch.each do |mutation|
      case mutation.first
      when :remove
        _remove(*mutation[1])
      else
        _mutate(*mutation)
      end
    end
  ensure
    @batch = nil
  end

  protected

  def client
    if @client.nil? || @client.current_server.nil?
      reconnect!
      @client.set_keyspace(@keyspace) if check_keyspace
    end
    @client
  end

  def reconnect!
    @servers = all_nodes
    @client = new_client
  end

  def check_keyspace(ks = @keyspace)
    !(unless (_keyspaces = keyspaces()).include?(ks)
      raise AccessError, "Keyspace #{ks.inspect} not found. Available: #{_keyspaces.inspect}"
    end)
  end

  def all_nodes
    if @auto_discover_nodes && !@keyspace.eql?("system")
      ips = (new_client.describe_ring(@keyspace).map {|range| range.endpoints}).flatten.uniq
      port = @servers.first.split(':').last
      ips.map{|ip| "#{ip}:#{port}" }
    else
      @servers
    end
  end

end
