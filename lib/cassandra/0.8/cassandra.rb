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
    client.set_keyspace(ks)
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
    client.truncate(column_family.to_s)
  end
  alias clear_column_family! truncate!

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

  def update_column_family(cf_def)
    begin
      res = client.system_update_column_family(cf_def)
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

  def update_keyspace(ks_def)
    begin
      res = client.system_update_keyspace(ks_def)
    rescue CassandraThrift::TimedOutException => toe
      puts "Timed out: #{toe.inspect}"
    rescue Thrift::TransportException => te
      puts "Timed out: #{te.inspect}"
    end
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

### 2ary Indexing

  def create_index(ks_name, cf_name, c_name, v_class)
    cf_def = client.describe_keyspace(ks_name).cf_defs.find{|x| x.name == cf_name}
    if !cf_def.nil? and !cf_def.column_metadata.find{|x| x.name == c_name}
      c_def  = CassandraThrift::ColumnDef.new do |cd|
        cd.name             = c_name
        cd.validation_class = "org.apache.cassandra.db.marshal."+v_class
        cd.index_type       = CassandraThrift::IndexType::KEYS
      end
      cf_def.column_metadata.push(c_def)
      update_column_family(cf_def)
    end
  end

  def drop_index(ks_name, cf_name, c_name)
    cf_def = client.describe_keyspace(ks_name).cf_defs.find{|x| x.name == cf_name}
    if !cf_def.nil? and cf_def.column_metadata.find{|x| x.name == c_name}
      cf_def.column_metadata.delete_if{|x| x.name == c_name}
      update_column_family(cf_def)
    end
  end

  def create_idx_expr(c_name, value, op)
    CassandraThrift::IndexExpression.new(
      :column_name => c_name,
      :value => value,
      :op => (case op
                when nil, "EQ", "eq", "=="
                  CassandraThrift::IndexOperator::EQ
                when "GTE", "gte", ">="
                  CassandraThrift::IndexOperator::GTE
                when "GT", "gt", ">"
                  CassandraThrift::IndexOperator::GT
                when "LTE", "lte", "<="
                  CassandraThrift::IndexOperator::LTE
                when "LT", "lt", "<"
                  CassandraThrift::IndexOperator::LT
              end ))
  end

  def create_idx_clause(idx_expressions, start = "")
    CassandraThrift::IndexClause.new(
      :start_key => start,
      :expressions => idx_expressions)
  end

  # TODO: Supercolumn support.
  def get_indexed_slices(column_family, idx_clause, *columns_and_options)
    column_family, columns, _, options =
      extract_and_validate_params(column_family, [], columns_and_options, READ_DEFAULTS)
    _get_indexed_slices(column_family, idx_clause, columns, options[:count], options[:start],
      options[:finish], options[:reversed], options[:consistency])
  end

  protected

  def client
    if @client.nil? || @client.current_server.nil?
      reconnect!
      @client.set_keyspace(@keyspace)
    end
    @client
  end

  def reconnect!
    @servers = all_nodes
    @client = new_client
  end

  def all_nodes
    if @auto_discover_nodes && !@keyspace.eql?("system")
      temp_client = new_client
      begin
        ips = (temp_client.describe_ring(@keyspace).map {|range| range.endpoints}).flatten.uniq
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
