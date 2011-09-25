
=begin rdoc
Create a new Cassandra client instance. Accepts a keyspace name, and optional host and port.

  client = Cassandra.new('twitter', '127.0.0.1:9160')
  
If the server requires authentication, you must authenticate before make calls

  client.login!('username','password')

You can then make calls to the server via the <tt>client</tt> instance.

  client.insert(:UserRelationships, "5", {"user_timeline" => {SimpleUUID::UUID.new => "1"}})
  client.get(:UserRelationships, "5", "user_timeline")

For read methods, valid option parameters are:

<tt>:count</tt>:: How many results to return. Defaults to 100.
<tt>:start</tt>:: Column name token at which to start iterating, inclusive. Defaults to nil, which means the first column in the collation order.
<tt>:finish</tt>:: Column name token at which to stop iterating, inclusive. Defaults to nil, which means no boundary.
<tt>:reversed</tt>:: Swap the direction of the collation order.
<tt>:consistency</tt>:: The consistency level of the request. Defaults to <tt>Cassandra::Consistency::ONE</tt> (one node must respond). Other valid options are <tt>Cassandra::Consistency::ZERO</tt>, <tt>Cassandra::Consistency::QUORUM</tt>, and <tt>Cassandra::Consistency::ALL</tt>.

Note that some read options have no relevance in some contexts.

For write methods, valid option parameters are:

<tt>:timestamp </tt>:: The transaction timestamp. Defaults to the current time in milliseconds. This is used for conflict resolution by the server; you normally never need to change it.
<tt>:consistency</tt>:: See above.

For the initial client instantiation, you may also pass in <tt>:thrift_client<tt> with a ThriftClient subclass attached. On connection, that class will be used instead of the default ThriftClient class, allowing you to add additional behavior to the connection (e.g. query logging).

=end

class Cassandra
  include Columns
  include Protocol
  include Helpers

  class AccessError < StandardError #:nodoc:
  end

  module Consistency
    include CassandraThrift::ConsistencyLevel
  end

  WRITE_DEFAULTS = {
    :count => 1000,
    :timestamp => nil,
    :consistency => Consistency::ONE,
    :ttl => nil
  }

  READ_DEFAULTS = {
    :count => 100,
    :start => nil,
    :finish => nil,
    :reversed => false,
    :consistency => Consistency::ONE
  }

  THRIFT_DEFAULTS = {
    :transport_wrapper    => Thrift::FramedTransport,
    :thrift_client_class  => ThriftClient
  }

  attr_reader :keyspace, :servers, :schema, :thrift_client_options, :thrift_client_class, :auth_request

  def self.DEFAULT_TRANSPORT_WRAPPER
    Thrift::FramedTransport
  end

  # Create a new Cassandra instance and open the connection.
  def initialize(keyspace, servers = "127.0.0.1:9160", thrift_client_options = {})
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}
    @auto_discover_nodes = true
    thrift_client_options[:transport_wrapper] ||= Cassandra.DEFAULT_TRANSPORT_WRAPPER
    @thrift_client_options = THRIFT_DEFAULTS.merge(thrift_client_options)
    @thrift_client_class = @thrift_client_options[:thrift_client_class]
    @keyspace = keyspace
    @servers = Array(servers)
  end

  ##
  # This method will prevent us from trying to auto-discover all the
  # server addresses, and only use the list of servers provided on
  # initialization.

  # This is primarily helpful when the cassandra cluster is communicating
  # internally on a different ip address than what you are using to connect.
  # A prime example of this would be when using EC2 to host a cluster.
  # Typically, the cluster would be communicating over the local ip
  # addresses issued by Amazon, but any clients connecting from outside EC2
  # would need to use the public ip.
  #
  def disable_node_auto_discovery!
    @auto_discover_nodes = false
  end

  ##
  # Disconnect the current client connection.
  #
  def disconnect!
    if @client
      @client.disconnect!
      @client = nil
    end
  end

  ##
  # Issues a login attempt using the username and password specified.
  #
  # * username
  # * password
  #
  def login!(username, password)
    request = CassandraThrift::AuthenticationRequest.new
    request.credentials = {'username' => username, 'password' => password}
    ret = client.login(request)

    # To avoid a double login on the initial connect, we set
    # @auth_request after the first successful login.
    #
    @auth_request = request
    ret
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      Array(schema(false).cf_defs).map {|cfdef| ":#{cfdef.name} => #{cfdef.column_type}"}.join(', ')
    }}, @servers=#{servers.inspect}>"
  end

  ##
  # Set the keyspace to use.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def keyspace=(ks)
    return false if Cassandra.VERSION.to_f < 0.7

    client.set_keyspace(ks)
    @schema = nil; @keyspace = ks
  end

  ##
  # Return an array of the keyspace names available.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def keyspaces
    return false if Cassandra.VERSION.to_f < 0.7

    client.describe_keyspaces.to_a.collect {|ksdef| ksdef.name }
  end

  ##
  # Return a hash of column_family definitions indexed by their
  # names
  def column_families
    return false if Cassandra.VERSION.to_f < 0.7

    schema.cf_defs.inject(Hash.new){|memo, cf_def| memo[cf_def.name] = cf_def; memo;}
  end

  ##
  # Return a Cassandra::Keyspace object loaded with the current
  # keyspaces schema.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def schema(load=true)
    return false if Cassandra.VERSION.to_f < 0.7

    if !load && !@schema
      Cassandra::Keyspace.new
    else
      @schema ||= client.describe_keyspace(@keyspace)
    end
  end

  ##
  # This returns true if all servers are in agreement on the schema.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def schema_agreement?
    return false if Cassandra.VERSION.to_f < 0.7

    client.describe_schema_versions().length == 1
  end

  ##
  # Lists the current cassandra.thrift version.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def version
    return false if Cassandra.VERSION.to_f < 0.7

    client.describe_version()
  end

  ##
  # Returns the string name specified for the cluster.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def cluster_name
    return false if Cassandra.VERSION.to_f < 0.7

    @cluster_name ||= client.describe_cluster_name()
  end

  ##
  # Returns an array of CassandraThrift::TokenRange objects indicating
  # which servers make up the current ring. What their start and end
  # tokens are, and their list of endpoints.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def ring
    return false if Cassandra.VERSION.to_f < 0.7

    client.describe_ring(@keyspace)
  end

  ##
  # Returns a string identifying which partitioner is in use by the
  # current cluster.  Typically, this will be RandomPartitioner, but it
  # could be OrderPreservingPartioner as well.
  #
  # Please note that this only works on version 0.7.0 and higher.
  def partitioner
    return false if Cassandra.VERSION.to_f < 0.7

    client.describe_partitioner()
  end

  ##
  # Remove all rows in the column family you request.
  #
  # * column_family
  # * options
  #   * consitency
  #   * timestamp
  #
  def truncate!(column_family)
    client.truncate(column_family.to_s)
  end
  alias clear_column_family! truncate!

  ##
  # Remove all column families in the keyspace.
  #
  # This method calls Cassandra#truncate! for each column family in the
  # keyspace.
  #
  # Please note that this only works on version 0.7.0 and higher.
  #
  def clear_keyspace!
    return false if Cassandra.VERSION.to_f < 0.7

    schema.cf_defs.each { |cfdef| truncate!(cfdef.name) }
  end

  ##
  # Creates a new column family from the passed in
  # Cassandra::ColumnFamily instance, and returns the schema id.
  #
  def add_column_family(cf_def)
    return false if Cassandra.VERSION.to_f < 0.7

    begin
      res = client.system_add_column_family(cf_def)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  ##
  # Delete the specified column family.  Return the new schema id.
  #
  # * column_family - The column_family name to drop.
  #
  def drop_column_family(column_family)
    return false if Cassandra.VERSION.to_f < 0.7

    begin
      res = client.system_drop_column_family(column_family)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  ##
  # Rename a column family. Returns the new schema id.
  #
  # * old_name - The current column_family name.
  # * new_name - The desired column_family name.
  #
  def rename_column_family(old_name, new_name)
    return false if Cassandra.VERSION.to_f != 0.7

    begin
      res = client.system_rename_column_family(old_name, new_name)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  ##
  # Update the column family based on the passed in definition.
  #
  def update_column_family(cf_def)
    return false if Cassandra.VERSION.to_f < 0.7

    begin
      res = client.system_update_column_family(cf_def)
    rescue CassandraThrift::TimedOutException => te
      puts "Timed out: #{te.inspect}"
    end
    @schema = nil
    res
  end

  ##
  # Add keyspace using the passed in keyspace definition.
  #
  # Returns the new schema id.
  #
  def add_keyspace(ks_def)
    return false if Cassandra.VERSION.to_f < 0.7

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

  ##
  # Deletes keyspace using the passed in keyspace name.
  #
  # Returns the new schema id.
  #
  def drop_keyspace(keyspace)
    return false if Cassandra.VERSION.to_f < 0.7

    begin
      res = client.system_drop_keyspace(keyspace)
    rescue CassandraThrift::TimedOutException => toe
      puts "Timed out: #{toe.inspect}"
    rescue Thrift::TransportException => te
      puts "Timed out: #{te.inspect}"
    end
    keyspace = "system" if keyspace.eql?(@keyspace)
    @keyspaces = nil
    res
  end

  ##
  # Renames keyspace.
  #
  # * old_name - Current keyspace name.
  # * new_name - Desired keyspace name.
  #
  # Returns the new schema id
  def rename_keyspace(old_name, new_name)
    return false if Cassandra.VERSION.to_f < 0.7

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

  ##
  # Update the keyspace using the passed in keyspace definition.
  #
  def update_keyspace(ks_def)
    return false if Cassandra.VERSION.to_f < 0.7

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
  ##
  # The initial default consistency is set to ONE, but you can use this method
  # to override the normal default with your specified value. Use this if you
  # do not want to specify a write consistency for each insert statement.
  #
  def default_write_consistency=(value)
    WRITE_DEFAULTS[:consistency] = value
  end

  ##
  # The initial default consistency is set to ONE, but you can use this method
  # to override the normal default with your specified value. Use this if you
  # do not want to specify a read consistency for each query.
  #
  def default_read_consistency=(value)
    READ_DEFAULTS[:consistency] = value
  end

  ##
  # This is the main method used to insert rows into cassandra. If the
  # column\_family that you are inserting into is a SuperColumnFamily then
  # the hash passed in should be a nested hash, otherwise it should be a
  # flat hash.
  #
  # This method can also be called while in batch mode. If in batch mode
  # then we queue up the mutations (an insert in this case) and pass them to
  # cassandra in a single batch at the end of the block.
  #
  # * column\_family - The column\_family that you are inserting into.
  # * key - The row key to insert.
  # * hash - The columns or super columns to insert.
  # * options - Valid options are:
  #   * :timestamp - Uses the current time if none specified.
  #   * :consistency - Uses the default write consistency if none specified.
  #   * :ttl - If specified this is the number of seconds after the insert that this value will be available.
  #
  def insert(column_family, key, hash, options = {})
    column_family, _, _, options = extract_and_validate_params(column_family, key, [options], WRITE_DEFAULTS)

    timestamp = options[:timestamp] || Time.stamp
    mutation_map = if is_super(column_family)
      {
        key => {
          column_family => hash.collect{|k,v| _super_insert_mutation(column_family, k, v, timestamp, options[:ttl]) }
        }
      }
    else
      {
        key => {
          column_family => hash.collect{|k,v| _standard_insert_mutation(column_family, k, v, timestamp, options[:ttl])}
        }
      }
    end

    @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
  end


  ##
  # This method is used to delete (actually marking them as deleted with a
  # tombstone)  rows, columns, or super columns depending on the parameters 
  # passed.  If only a key is passed the entire row will be marked as deleted.
  # If a column name is passed in that column will be deleted.
  #
  # This method can also be used in batch mode. If in batch mode then we
  # queue up the mutations (a deletion in this case)
  #
  # * column\_family - The column\_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub\_columns to select.
  # * options - Valid options are:
  #   * :timestamp - Uses the current time if none specified.
  #   * :consistency - Uses the default write consistency if none specified.
  #
  # TODO: we could change this function or add another that support multi-column removal (by list or predicate)
  #
  def remove(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    if @batch
      mutation_map = 
        {
          key => {
            column_family => [ _delete_mutation(column_family, column, sub_column, options[:timestamp]|| Time.stamp) ]
          }
        }
      @batch << [mutation_map, options[:consistency]]
    else 
      # Let's continue using the 'remove' thrift method...not sure about the implications/performance of using the mutate instead
      # Otherwise we coul get use the mutation_map above, and do _mutate(mutation_map, options[:consistency])
      args = {:column_family => column_family}
      columns = is_super(column_family) ? {:super_column => column, :column => sub_column} : {:column => column}
      column_path = CassandraThrift::ColumnPath.new(args.merge(columns))
      _remove(key, column_path, options[:timestamp] || Time.stamp, options[:consistency])
    end
  end

  ##
  # Count the columns for the provided parameters.
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :start - The column name to start from.
  #   * :stop - The column name to stop at.
  #   * :count - The maximum count of columns to return. (By default cassandra will count up to 100 columns)
  #   * :consistency - Uses the default read consistency if none specified.
  #
  def count_columns(column_family, key, *columns_and_options)
    column_family, super_column, _, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _count_columns(column_family, key, super_column, options[:start], options[:stop], options[:count], options[:consistency])
  end

  ##
  # Multi-key version of Cassandra#count_columns. Please note that this
  # queries the server for each key passed in.
  #
  # Supports same parameters as Cassandra#count_columns.
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :consistency - Uses the default read consistency if none specified.
  #
  # FIXME: Not real multi; needs server support
  def multi_count_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, count_columns(column_family, key, *options)] }._flatten_once]
  end

  ##
  # Return a hash of column value pairs for the path you request. 
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :consistency - Uses the default read consistency if none specified.
  #
  def get_columns(column_family, key, *columns_and_options)
    column_family, columns, sub_columns, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _get_columns(column_family, key, columns, sub_columns, options[:consistency])
  end

  ##
  # Multi-key version of Cassandra#get_columns. Please note that this
  # queries the server for each key passed in.
  #
  # Supports same parameters as Cassandra#get_columns
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :consistency - Uses the default read consistency if none specified.
  #
  # FIXME Not real multi; needs to use a Column predicate
  def multi_get_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, get_columns(column_family, key, *options)] }._flatten_once]
  end

  ##
  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request. 
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :count    - The number of columns requested to be returned.
  #   * :start    - The starting value for selecting a range of columns.
  #   * :finish   - The final value for selecting a range of columns.
  #   * :reversed - If set to true the results will be returned in
  #                 reverse order.
  #   * :consistency - Uses the default read consistency if none specified.
  #
  def get(column_family, key, *columns_and_options)
    multi_get(column_family, [key], *columns_and_options)[key]
  end

  ##
  # Multi-key version of Cassandra#get.
  #
  # This method allows you to select multiple rows with a single query.
  # If a key that is passed in doesn't exist an empty hash will be
  # returned.
  #
  # Supports the same parameters as Cassandra#get.
  #
  # * column_family   - The column_family that you are inserting into.
  # * keys            - An array of keys to select.
  # * columns         - Either a single super_column or a list of columns.
  # * sub_columns     - The list of sub_columns to select.
  # * options         - Valid options are:
  #   * :count        - The number of columns requested to be returned.
  #   * :start        - The starting value for selecting a range of columns.
  #   * :finish       - The final value for selecting a range of columns.
  #   * :reversed     - If set to true the results will be returned in reverse order.
  #   * :consistency  - Uses the default read consistency if none specified.
  #
  def multi_get(column_family, keys, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, keys, columns_and_options, READ_DEFAULTS)

    hash = _multiget(column_family, keys, column, sub_column, options[:count], options[:start], options[:finish], options[:reversed], options[:consistency])

    # Restore order
    ordered_hash = OrderedHash.new
    keys.each { |key| ordered_hash[key] = hash[key] || (OrderedHash.new if is_super(column_family) and !sub_column) }
    ordered_hash
  end

  ##
  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists.
  #
  # If passed in only a row key it will query for any columns (limiting
  # to 1) for that row key. If a column is passed in it will query for
  # that specific column/super column.
  #
  # This method will return true or false.
  #
  # * column_family - The column_family that you are inserting into.
  # * key - The row key to insert.
  # * columns - Either a single super_column or a list of columns.
  # * sub_columns - The list of sub_columns to select.
  # * options - Valid options are:
  #   * :consistency - Uses the default read consistency if none specified.
  #
  def exists?(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)
    result = if column
               _multiget(column_family, [key], column, sub_column, 1, '', '', false, options[:consistency])[key]
             else
               _multiget(column_family, [key], nil, nil, 1, '', '', false, options[:consistency])[key]
             end

    ![{}, nil].include?(result)
  end

  ##
  # Return an Cassandra::OrderedHash containing the columns specified for the given
  # range of keys in the column_family you request.
  #
  # This method is just a convenience wrapper around Cassandra#get_range_single
  # and Cassandra#get_range_batch. If :key_size, :batch_size, or a block
  # is passed in Cassandra#get_range_batch will be called. Otherwise
  # Cassandra#get_range_single will be used.
  #
  # The start_key and finish_key parameters are only useful for iterating of all records
  # as is done in the Cassandra#each and Cassandra#each_key methods if you are using the 
  # RandomPartitioner.
  #
  # If the table is partitioned with OrderPreservingPartitioner you may
  # use the start_key and finish_key params to select all records with
  # the same prefix value.
  #
  # If a block is passed in we will yield the row key and columns for
  # each record returned.
  #
  # Please note that Cassandra returns a row for each row that has existed in the
  # system since gc_grace_seconds. This is because deleted row keys are marked as 
  # deleted, but left in the system until the cluster has had resonable time to replicate the deletion.
  # This function attempts to suppress deleted rows (actually any row returned without
  # columns is suppressed).
  # 
  # Please note that when enabling the :reversed option, :start and :finish should be swapped (e.g.
  # reversal happens before selecting the range).
  #
  # * column_family - The column_family that you are inserting into.
  # * options - Valid options are:
  #   * :start_key    - The starting value for selecting a range of keys (only useful with OPP).
  #   * :finish_key   - The final value for selecting a range of keys (only useful with OPP).
  #   * :key_count    - The total number of keys to return from the query. (see note regarding deleted records)
  #   * :batch_size   - The maximum number of keys to return per query. If specified will loop until :key_count is obtained or all records have been returned.
  #   * :columns 	    - A list of columns to return.
  #   * :count        - The number of columns requested to be returned.
  #   * :start        - The starting value for selecting a range of columns.
  #   * :finish       - The final value for selecting a range of columns.
  #   * :reversed     - If set to true the results will be returned in reverse order.
  #   * :consistency  - Uses the default read consistency if none specified.
  #
  def get_range(column_family, options = {}, &blk)
    if block_given? || options[:key_count] || options[:batch_size]
      get_range_batch(column_family, options, &blk)
    else
      get_range_single(column_family, options, &blk)
    end
  end

  ##
  # Return an Cassandra::OrderedHash containing the columns specified for the given
  # range of keys in the column_family you request.
  #
  # See Cassandra#get_range for more details.
  #
  def get_range_single(column_family, options = {})
    return_empty_rows = options.delete(:return_empty_rows) || false

    column_family, _, _, options = 
      extract_and_validate_params(column_family, "", [options], 
                                  READ_DEFAULTS.merge(:start_key  => '',
                                                      :finish_key => '',
                                                      :key_count  => 100,
                                                      :columns    => nil,
                                                      :reversed   => false
                                                     )
                                 )

    results = _get_range( column_family,
                          options[:start_key].to_s,
                          options[:finish_key].to_s,
                          options[:key_count],
                          options[:columns],
                          options[:start].to_s,
                          options[:finish].to_s,
                          options[:count],
                          options[:consistency],
                          options[:reversed] )

    multi_key_slices_to_hash(column_family, results, return_empty_rows)
  end

  ##
  # Return an Cassandra::OrderedHash containing the columns specified for the given
  # range of keys in the column_family you request.
  #
  # If a block is passed in we will yield the row key and columns for
  # each record returned and return a nil value instead of a Cassandra::OrderedHash.
  #
  # See Cassandra#get_range for more details.
  #
  def get_range_batch(column_family, options = {})
    batch_size    = options.delete(:batch_size) || 100
    count         = options.delete(:key_count)
    result        = (!block_given? && {}) || nil
    num_results    = 0

    options[:start_key] ||= ''
    last_key  = nil

    while options[:start_key] != last_key && (count.nil? || count > num_results)
      options[:start_key] = last_key
      res = get_range_single(column_family, options.merge!(:start_key => last_key,
                                                           :key_count => batch_size,
                                                           :return_empty_rows => true
                                                          ))
      res.each do |key, columns|
        next if options[:start_key] == key
        next if num_results == count

        unless columns == {}
          if block_given?
            yield key, columns
          else
            result[key] = columns
          end
          num_results += 1
        end
        last_key = key
      end
    end

    result
  end

  ##
  # Count all rows in the column_family you request.
  #
  # This method just calls Cassandra#get_range_keys and returns the
  # number of records returned.
  #
  # See Cassandra#get_range for options.
  #
  def count_range(column_family, options = {})
    get_range_keys(column_family, options).length
  end

  ##
  # Return an Array containing all of the keys within a given range.
  #
  # This method just calls Cassandra#get_range and returns the
  # row keys for the records returned.
  #
  # See Cassandra#get_range for options.
  #
  def get_range_keys(column_family, options = {})
    get_range(column_family,options.merge!(:count => 1)).keys
  end

  ##
  # Iterate through each key within the given parameters. This function can be
  # used to iterate over each key in the given column family.
  #
  # This method just calls Cassandra#get_range and yields each row key.
  #
  # See Cassandra#get_range for options.
  #
  def each_key(column_family, options = {})
    get_range_batch(column_family, options) do |key, columns|
      yield key
    end
  end

  ##
  # Iterate through each row in the given column family
  #
  # This method just calls Cassandra#get_range and yields the key and
  # columns.
  #
  # See Cassandra#get_range for options.
  #
  def each(column_family, options = {})
    get_range_batch(column_family, options) do |key, columns|
      yield key, columns
    end
  end

  ##
  # Open a batch operation and yield self. Inserts and deletes will be queued
  # until the block closes, and then sent atomically to the server.
  #
  # Supports the :consistency option, which overrides the consistency set in
  # the individual commands.
  #
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

  ##
  # Create secondary index.
  #
  # * keyspace
  # * column_family
  # * column_name
  # * validation_class
  #
  def create_index(keyspace, column_family, column_name, validation_class)
    return false if Cassandra.VERSION.to_f < 0.7

    cf_def = client.describe_keyspace(keyspace).cf_defs.find{|x| x.name == column_family}
    if !cf_def.nil? and !cf_def.column_metadata.find{|x| x.name == column_name}
      c_def  = CassandraThrift::ColumnDef.new do |cd|
        cd.name             = column_name
        cd.validation_class = "org.apache.cassandra.db.marshal."+validation_class
        cd.index_type       = CassandraThrift::IndexType::KEYS
      end
      cf_def.column_metadata.push(c_def)
      update_column_family(cf_def)
    end
  end

  ##
  # Delete secondary index.
  #
  # * keyspace
  # * column_family
  # * column_name
  #
  def drop_index(keyspace, column_family, column_name)
    return false if Cassandra.VERSION.to_f < 0.7

    cf_def = client.describe_keyspace(keyspace).cf_defs.find{|x| x.name == column_family}
    if !cf_def.nil? and cf_def.column_metadata.find{|x| x.name == column_name}
      cf_def.column_metadata.delete_if{|x| x.name == column_name}
      update_column_family(cf_def)
    end
  end

  ##
  # This method is mostly used internally by get_index_slices to create
  # a CassandraThrift::IndexExpression for the given options.
  #
  # * column_name - Column to be compared
  # * value       - Value to compare against
  # * comparison  - Type of comparison to do.
  #
  def create_index_expression(column_name, value, comparison)
    return false if Cassandra.VERSION.to_f < 0.7

    CassandraThrift::IndexExpression.new(
      :column_name => column_name,
      :value => value,
      :op => (case comparison
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
  alias :create_idx_expr :create_index_expression

  ##
  # This method takes an array if CassandraThrift::IndexExpression
  # objects and creates a CassandraThrift::IndexClause for use in the
  # Cassandra#get_index_slices
  #
  # * index_expressions - Array of CassandraThrift::IndexExpressions.
  # * start             - The starting row key.
  # * count             - The count of items to be returned
  #
  def create_index_clause(index_expressions, start = "", count = 100)
    return false if Cassandra.VERSION.to_f < 0.7

    CassandraThrift::IndexClause.new(
      :start_key    => start,
      :expressions  => index_expressions,
      :count        => count)
  end
  alias :create_idx_clause :create_index_clause

  ##
  # This method is used to query a secondary index with a set of
  # provided search parameters
  #
  # Please note that you can either specify a
  # CassandraThrift::IndexClause or an array of hashes with the
  # format as below.
  #
  # * column_family     - The Column Family this operation will be run on.
  # * index_clause      - This can either be a CassandraThrift::IndexClause or an array of hashes with the following keys:
  #   * :column_name - Column to be compared
  #   * :value       - Value to compare against
  #   * :comparison  - Type of comparison to do.
  # * options
  #   * :key_count    - Set maximum number of rows to return. (Only works if CassandraThrift::IndexClause is not passed in.)
  #   * :key_start    - Set starting row key for search. (Only works if CassandraThrift::IndexClause is not passed in.)
  #   * :consistency
  #
  # TODO: Supercolumn support.
  def get_indexed_slices(column_family, index_clause, *columns_and_options)
    return false if Cassandra.VERSION.to_f < 0.7

    column_family, columns, _, options =
      extract_and_validate_params(column_family, [], columns_and_options, READ_DEFAULTS.merge(:key_count => 100, :key_start => ""))

    if index_clause.class != CassandraThrift::IndexClause
      index_expressions = index_clause.collect do |expression|
        create_index_expression(expression[:column_name], expression[:value], expression[:comparison])
      end

      index_clause = create_index_clause(index_expressions, options[:key_start], options[:key_count])
    end

    key_slices = _get_indexed_slices(column_family, index_clause, columns, options[:count], options[:start],
      options[:finish], options[:reversed], options[:consistency])

    key_slices.inject({}){|h, key_slice| h[key_slice.key] = key_slice.columns; h}
  end

  protected

  def calling_method
    "#{self.class}##{caller[0].split('`').last[0..-3]}"
  end

  ##
  # Roll up queued mutations, to improve atomicity (and performance).
  #
  def compact_mutations!
    used_clevels = {} # hash that lists the consistency levels seen in the batch array. key is the clevel, value is true
    by_key = Hash.new{|h,k | h[k] = {}}
    # @batch is an array of mutation_ops.
    # A mutation op is a 2-item array containing [mutationmap, consistency_number]
    # a mutation map is a hash, by key (string) that has a hash by CF name, containing a list of column_mutations)
    @batch.each do |mutation_op|
      # A single mutation op looks like:
      # For an insert/update
      #[ { key1 => 
      #            { CF1 => [several of CassThrift:Mutation(colname,value,TS,ttl)]
      #              CF2 => [several mutations]
      #            },
      #    key2 => {...} # Not sure if they can come batched like this...so there might only be a single key (and CF)
      #      }, # [0]
      #  consistency # [1] 
      #]
      mmap = mutation_op[0] # :remove OR a hash like {"key"=> {"CF"=>[mutationclass1,...] } }
      used_clevels[mutation_op[1]] = true #save the clevel required for this operation

      mmap.keys.each do |k|
        mmap[k].keys.each do |cf| # For each CF in that key
          by_key[k][cf] ||= []
          by_key[k][cf].concat(mmap[k][cf]) # Append the list of mutations for that key and CF
        end
      end
    end
    # Returns the batch mutations map, and an array with the consistency levels 'seen' in the batch
    [by_key, used_clevels.keys]
  end

  ##
  # Creates a new client as specified by Cassandra.thrift_client_options[:thrift_client_class]
  #
  def new_client
    thrift_client_class.new(CassandraThrift::Cassandra::Client, @servers, @thrift_client_options)
  end

  def client
    if @client.nil? || @client.current_server.nil?
      reconnect!
    end
    @client
  end

  def reconnect!
    @servers = all_nodes
    @client = new_client
    @client.add_callback :post_connect do |cli|
      # Set the active keyspace after connecting
      cli.set_keyspace(@keyspace)

      # If using an authenticated keyspace, ensure we relogin
      cli.login(@auth_request) if @auth_request
    end
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
