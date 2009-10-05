
=begin rdoc
Create a new Cassandra client instance. Accepts a keyspace name, and optional host and port.

  client = Cassandra.new('twitter', '127.0.0.1', 9160)

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

  class AccessError < StandardError #:nodoc:
  end

  module Consistency
    include CassandraThrift::ConsistencyLevel
  end

  MAX_INT = 2**31 - 1

  WRITE_DEFAULTS = {
    :count => MAX_INT,
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

  attr_reader :keyspace, :host, :port, :serializer, :transport

  # Instantiate a new Cassandra and open the connection.
  def initialize(keyspace, host = '127.0.0.1', port = 9160, buffer = true)
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}

    @keyspace = keyspace
    @host = host
    @port = port
    @buffer = buffer
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @host=#{host.inspect}, @port=#{port}>"
  end

### Write

  # Insert a row for a key. Pass a flat hash for a regular column family, and
  # a nested hash for a super column family. Supports the <tt>:consistency</tt>
  # and <tt>:timestamp</tt> options.
  def insert(column_family, key, hash, options = {})
    column_family, _, _, options = 
      validate_params(column_family, key, [options], WRITE_DEFAULTS)

    args = [column_family, hash, options[:timestamp] || Time.stamp]
    columns = is_super(column_family) ? hash_to_super_columns(*args) : hash_to_columns(*args)
    mutation = CassandraThrift::BatchMutation.new(
      :key => key,
      :cfmap => {column_family => columns},
      :column_paths => [])

    @batch ? @batch << mutation : _mutate([mutation], options[:consistency])
  end

  ## Delete

  # _mutate the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports the <tt>:consistency</tt> and <tt>:timestamp</tt>
  # options.
  def remove(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = 
      validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    args = {:column_family => column_family, :timestamp => options[:timestamp] || Time.stamp}
    columns = is_super(column_family) ? {:super_column => column, :column => sub_column} : {:column => column}
    mutation = CassandraThrift::BatchMutation.new(
      :key => key,
      :cfmap => {},
      :column_paths => [CassandraThrift::ColumnPath.new(args.merge(columns))])

    @batch ? @batch << mutation : _mutate([mutation], options[:consistency])
  end

  # Remove all rows in the column family you request. Supports options
  # <tt>:consistency</tt> and <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting
  # for ranged remove support in Cassandra.
  def clear_column_family!(column_family, options = {})
    get_range(column_family).each { |key| remove(column_family, key, options) }
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
      validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
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
      validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
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
      validate_params(column_family, keys, columns_and_options, READ_DEFAULTS)

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
      validate_params(column_family, key, columns_and_options, READ_DEFAULTS)
    _multiget(column_family, [key], column, sub_column, 1, nil, nil, nil, options[:consistency])[key]
  end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash. Supports the
  # <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, and <tt>:consistency</tt>
  # options.
  def get_range(column_family, options = {})
    column_family, _, _, options = 
      validate_params(column_family, "", [options], READ_DEFAULTS)
    _get_range(column_family, options[:start].to_s, options[:finish].to_s, options[:count], options[:consistency])
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash. Supports the <tt>:start</tt>,
  # <tt>:finish</tt>, and <tt>:consistency</tt> options.
  # FIXME will count only MAX_INT records
  def count_range(column_family, options = {})
    get_range(column_family, options.merge(:count => MAX_INT)).size
  end

  # Open a batch operation and yield. Inserts and deletes will be queued until
  # the block closes, and then sent atomically to the server.  Supports the
  # <tt>:consistency</tt> option, which overrides the consistency set in
  # the individual commands.
  def batch(options = {})
    _, _, _, options = 
      validate_params(schema.keys.first, "", [options], WRITE_DEFAULTS)

    @batch = []
    yield
    compact_mutations!
    _mutate(@batch, options[:consistency])
  ensure
    @batch = nil
  end

  private

  # Extract and validate options.
  # FIXME Should be done as a decorator
  def validate_params(column_family, keys, args, options)
    options = options.dup
    column_family = column_family.to_s

    # Keys
    Array(keys).each do |key|      
      raise ArgumentError, "Key #{key.inspect} must be a String for #{calling_method}" unless key.is_a?(String)
    end
    
    # Options
    if args.last.is_a?(Hash)
      extras = args.last.keys - options.keys
      raise ArgumentError, "Invalid options #{extras.inspect[1..-2]} for #{calling_method}" if extras.any?
      options.merge!(args.pop)      
    end

    # Ranges
    column, sub_column = args[0], args[1]
    klass, sub_klass = column_name_class(column_family), sub_column_name_class(column_family)        
    range_class = column ? sub_klass : klass
    options[:start] = options[:start] ? range_class.new(options[:start]).to_s : ""
    options[:finish] = options[:finish] ? range_class.new(options[:finish]).to_s : ""
    
    [column_family, s_map(column, klass), s_map(sub_column, sub_klass), options]
  end
  
  def calling_method
     "#{self.class}##{caller[0].split('`').last[0..-3]}"
  end

  # Convert stuff to strings.
  def s_map(el, klass)
    case el
    when Array then el.map { |i| s_map(i, klass) }
    when NilClass then nil
    else
      klass.new(el).to_s
    end
  end

  # Roll up queued mutations, to improve atomicity.
  def compact_mutations!
    mutations = {}

    # Nested hash merge
    @batch.each do |m|
      if mutation = mutations[m.key]
        # Inserts
        if columns = mutation.cfmap[m.cfmap.keys.first]
          columns.concat(m.cfmap.values.first)
        else
          mutation.cfmap.merge!(m.cfmap)
        end
        # Deletes
        mutation.column_paths.concat(m.column_paths)
      else
        mutations[m.key] = m
      end
    end

    # FIXME Return atomic thrift thingy
    @batch = mutations.values
  end

  def schema
    @schema ||= client.describe_keyspace(@keyspace)
  end

  def client
    @client ||= begin
      transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
      transport.open
    
      client = CassandraThrift::Cassandra::SafeClient.new(
        CassandraThrift::Cassandra::Client.new(Thrift::BinaryProtocol.new(transport)),
        transport,
        !@buffer)

      keyspaces = client.get_string_list_property("keyspaces")
      unless keyspaces.include?(@keyspace)
        raise AccessError, "Keyspace #{@keyspace.inspect} not found. Available: #{keyspaces.inspect}"
      end

      client
    end
  end
end
