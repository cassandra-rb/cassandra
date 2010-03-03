
=begin rdoc
Create a new Cassandra client instance. Accepts a keyspace name, and optional host and port.

  client = Cassandra.new('twitter', '127.0.0.1:9160')

You can then make calls to the server via the <tt>client</tt> instance.

  client.insert(:UserRelationships, "5", {"user_timeline" => {UUID.new => "1"}})
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

=end rdoc

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
    :consistency => Consistency::ONE
  }.freeze

  READ_DEFAULTS = {
    :count => 100,
    :start => nil,
    :finish => nil,
    :reversed => false,
    :consistency => Consistency::ONE
  }.freeze
  
  THRIFT_DEFAULTS = {
    :transport_wrapper => Thrift::BufferedTransport
  }.freeze

  attr_reader :keyspace, :servers, :schema, :thrift_client_options

  # Create a new Cassandra instance and open the connection.
  def initialize(keyspace, servers = "127.0.0.1:9160", thrift_client_options = {})
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}
    @thrift_client_options = THRIFT_DEFAULTS.merge(thrift_client_options)

    @keyspace = keyspace
    @servers = Array(servers)
  end

  def disconnect!
    @client.disconnect!
    @client = nil
  end

  def keyspaces
    @keyspaces ||= client.get_string_list_property("keyspaces")
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema(false).map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @servers=#{servers.inspect}>"
  end

### Write

  # Insert a row for a key. Pass a flat hash for a regular column family, and
  # a nested hash for a super column family. Supports the <tt>:consistency</tt>
  # and <tt>:timestamp</tt> options.
  def insert(column_family, key, hash, options = {})
    column_family, _, _, options = extract_and_validate_params(column_family, key, [options], WRITE_DEFAULTS)

    timestamp = options[:timestamp] || Time.stamp
    cfmap = hash_to_cfmap(column_family, hash, timestamp)
    mutation = [:insert, [key, cfmap, options[:consistency]]]

    @batch ? @batch << mutation : _insert(*mutation[1])
  end

  ## Delete

  # _mutate the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports the <tt>:consistency</tt> and <tt>:timestamp</tt>
  # options.
  def remove(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    args = {:column_family => column_family}
    columns = is_super(column_family) ? {:super_column => column, :column => sub_column} : {:column => column}
    column_path = CassandraThrift::ColumnPath.new(args.merge(columns))
    
    mutation = [:remove, [key, column_path, options[:timestamp] || Time.stamp, options[:consistency]]]
    
    @batch ? @batch << mutation : _remove(*mutation[1])
  end

  # Remove all rows in the column family you request. Supports options
  # <tt>:consistency</tt> and <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_column_family!(column_family, options = {})
    while (keys = get_range(column_family, :count => 100)).length > 0
      keys.each { |key| remove(column_family, key, options) }
    end
  end

  # Remove all rows in the keyspace. Supports options <tt>:consistency</tt> and
  # <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_keyspace!(options = {})
    schema.keys.each { |column_family| clear_column_family!(column_family, options) }
  end

### Read

  # Count the elements at the column_family:key:[super_column] path you
  # request. Supports the <tt>:consistency</tt> option.
  def count_columns(column_family, key, *columns_and_options)
    column_family, super_column, _, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _count_columns(column_family, key, super_column, options[:consistency])
  end

  # Multi-key version of Cassandra#count_columns. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  # FIXME Not real multi; needs server support
  def multi_count_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, count_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a list of single values for the elements at the
  # column_family:key:column[s]:[sub_columns] path you request. Supports the
  # <tt>:consistency</tt> option.
  def get_columns(column_family, key, *columns_and_options)
    column_family, columns, sub_columns, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _get_columns(column_family, key, columns, sub_columns, options[:consistency])
  end

  # Multi-key version of Cassandra#get_columns. Supports the <tt>:consistency</tt>
  # option.
  # FIXME Not real multi; needs to use a Column predicate
  def multi_get_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, get_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports options <tt>:count</tt>, <tt>:start</tt>,
  # <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def get(column_family, key, *columns_and_options)
    multi_get(column_family, [key], *columns_and_options)[key]
  end

  # Multi-key version of Cassandra#get. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_get(column_family, keys, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, keys, columns_and_options, READ_DEFAULTS)

    hash = _multiget(column_family, keys, column, sub_column, options[:count], options[:start], options[:finish], options[:reversed], options[:consistency])
    # Restore order
    ordered_hash = OrderedHash.new
    keys.each { |key| ordered_hash[key] = hash[key] || (OrderedHash.new if is_super(column_family) and !sub_column) }
    ordered_hash
  end

  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists. Supports the <tt>:consistency</tt> option.
  def exists?(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)
    _multiget(column_family, [key], column, sub_column, 1, nil, nil, nil, options[:consistency])[key]
  end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash. Supports the
  # <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, and <tt>:consistency</tt>
  # options.
  def get_range(column_family, options = {})
    column_family, _, _, options = 
      extract_and_validate_params(column_family, "", [options], READ_DEFAULTS)
    _get_range(column_family, options[:start].to_s, options[:finish].to_s, options[:count], options[:consistency])
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash. Supports the <tt>:start</tt>,
  # <tt>:finish</tt>, and <tt>:consistency</tt> options.
  def count_range(column_family, options = {})
    count = 0
    l = []
    start_key = options[:start]
    while (l = get_range(column_family, options.merge(:count => 1000, :start => start_key))).size > 0
      count += l.size
      start_key = l.last.succ
    end
    count
  end

  # Open a batch operation and yield. Inserts and deletes will be queued until
  # the block closes, and then sent atomically to the server.  Supports the
  # <tt>:consistency</tt> option, which overrides the consistency set in
  # the individual commands.
  def batch(options = {})
    _, _, _, options = 
      extract_and_validate_params(schema.keys.first, "", [options], WRITE_DEFAULTS)

    @batch = []
    yield
    compact_mutations!

    @batch.each do |mutation|
      case mutation.first
      when :insert
        _insert(*mutation[1])
      when :remove
        _remove(*mutation[1])
      end
    end
  ensure
    @batch = nil
  end

  protected

  def calling_method
    "#{self.class}##{caller[0].split('`').last[0..-3]}"
  end

  # Roll up queued mutations, to improve atomicity.
  def compact_mutations!
    #TODO re-do this rollup
  end

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

  def new_client
    ThriftClient.new(CassandraThrift::Cassandra::Client, @servers, @thrift_client_options)
  end

  def all_nodes
    ips = ::JSON.parse(new_client.get_string_property('token map')).values
    port = @servers.first.split(':').last
    ips.map{|ip| "#{ip}:#{port}" }
  end
end
