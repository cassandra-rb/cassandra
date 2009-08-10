
class Cassandra

=begin rdoc
Create a new Cassandra client instance. Accepts a keyspace name, and optional host and port.

  Cassandra.new('twitter', '127.0.0.1', 9160)
  
For write operations, valid option parameters usually are:
 
<tt>:consistency</tt>:: The consistency level of the request. Defaults to <tt>Cassandra::Consistency::WEAK</tt> (one node must respond). Other valid options are <tt>Cassandra::Consistency::NONE</tt>, <tt>Cassandra::Consistency::STRONG</tt>, and <tt>Cassandra::Consistency::PERFECT</tt>.
<tt>:timestamp </tt>:: The transaction timestamp. Defaults to the current time in milliseconds. This is used for conflict resolution by the server; you normally never need to change it.

For read operations, valid option parameters usually are:

<tt>:count</tt>:: How many results to return. Defaults to 100.
<tt>:start</tt>:: Column name token at which to start iterating, inclusive. Defaults to nil, which means the first column in the collation order.
<tt>:finish</tt>:: Column name token at which to stop iterating, inclusive. Defaults to nil, which means no boundary.
<tt>:reversed</tt>:: Swap the direction of the collation order.
<tt>:consistency</tt>:: The consistency level of the request. Defaults to <tt>Cassandra::Consistency::WEAK</tt> (one node must respond). Other valid options are <tt>Cassandra::Consistency::NONE</tt>, <tt>Cassandra::Consistency::STRONG</tt>, and <tt>Cassandra::Consistency::PERFECT</tt>.

=end rdoc

  include Columns
  include Protocol

  class AccessError < StandardError; end

  module Consistency
    include CassandraThrift::ConsistencyLevel
    NONE = ZERO
    WEAK = ONE
    STRONG = QUORUM
    PERFECT = ALL
  end

  MAX_INT = 2**31 - 1
    
  WRITE_DEFAULTS = {    
    :count => MAX_INT,
    :timestamp => nil,
    :consistency => Consistency::WEAK }

  READ_DEFAULTS = {
    :count => 100, 
    :start => nil, 
    :finish => nil, 
    :reversed => false, 
    :consistency => Consistency::WEAK }

  attr_reader :keyspace, :host, :port, :serializer, :transport, :client, :schema

  # Instantiate a new Cassandra and open the connection.
  def initialize(keyspace, host = '127.0.0.1', port = 9160)
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}

    @keyspace = keyspace
    @host = host
    @port = port

    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
    @transport.open
    @client = CassandraThrift::Cassandra::SafeClient.new(
      CassandraThrift::Cassandra::Client.new(Thrift::BinaryProtocol.new(@transport)),
      @transport)

    keyspaces = @client.get_string_list_property("keyspaces")
    unless keyspaces.include?(@keyspace)
      raise AccessError, "Keyspace #{@keyspace.inspect} not found. Available: #{keyspaces.inspect}"
    end

    @schema = @client.describe_keyspace(@keyspace)
  end

  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @host=#{host.inspect}, @port=#{port}>"
  end

  ## Write

  # Insert a row for a key. Pass a flat hash for a regular column family, and
  # a nested hash for a super column family. Supports the <tt>:consistency</tt>
  # and <tt>:timestamp</tt> options.
  def insert(column_family, key, hash, options = {})
    _, _, options = parse_options([options], WRITE_DEFAULTS)
    column_family = column_family.to_s    
    args = [column_family, hash, options[:timestamp] || Time.stamp]
    
    mutation = if is_super(column_family)
      CassandraThrift::BatchMutationSuper.new(
        :key => key, 
        :cfmap => {column_family => hash_to_super_columns(*args)})
    else
      CassandraThrift::BatchMutation.new(
        :key => key, 
        :cfmap => {column_family => hash_to_columns(*args)})
    end
    # FIXME Batched operations discard the consistency argument
    @batch ? @batch << mutation : _insert(mutation, options[:consistency])
  end

  ## Delete

  # Remove the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports the <tt>:consistency</tt> and <tt>:timestamp</tt>
  # options.
  def remove(column_family, key, *columns_and_options)
    column, sub_column, options = parse_options(columns_and_options, WRITE_DEFAULTS)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)

    column = column.to_s if column
    sub_column = sub_column.to_s if sub_column
    args = [column_family, key, column, sub_column, options[:consistency], options[:timestamp] || Time.stamp]
    @batch ? @batch << args : _remove(*args)
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
    @schema.keys.each { |column_family| clear_column_family!(column_family, options) }
  end

  ## Read

  # Count the elements at the column_family:key:[super_column] path you
  # request. Supports options <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, 
  # <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def count_columns(column_family, key, *columns_and_options)
    super_column, _, options = parse_options(columns_and_options, READ_DEFAULTS)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, super_column)
    
    _count_columns(column_family, key, super_column, options[:consistency])
  end

  # Multi-key version of Cassandra#count_columns. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_count_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, count_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a list of single values for the elements at the
  # column_family:key:column[s]:[sub_columns] path you request. Supports the 
  # <tt>:consistency</tt> option.
  def get_columns(column_family, key, *columns_and_options)
    columns, sub_columns, options = parse_options(columns_and_options, READ_DEFAULTS)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, columns, sub_columns)    
    
    _get_columns(column_family, key, columns, sub_columns, options[:consistency])
  end

  # Multi-key version of Cassandra#get_columns. Supports the <tt>:consistency</tt> 
  # option.
  def multi_get_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, get_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports options <tt>:count</tt>, <tt>:start</tt>, 
  # <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def get(column_family, key, *columns_and_options)
    column, sub_column, options = parse_options(columns_and_options, READ_DEFAULTS)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    
    _get(column_family, key, column, sub_column, options[:count], options[:start], options[:finish], options[:reversed], options[:consistency])
  rescue CassandraThrift::NotFoundException
    is_super(column_family) && !sub_column ? OrderedHash.new : nil
  end

  # Multi-key version of Cassandra#get. Supports options <tt>:count</tt>, 
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_get(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, get(column_family, key, *options)] }._flatten_once]
  end

  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists. Supports the <tt>:consistency</tt> option.
  def exists?(column_family, key, *columns_and_options)
    column, sub_column, options = parse_options(columns_and_options, READ_DEFAULTS)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    
    _get(column_family, key, column, sub_column, 1, nil, nil, nil, options[:consistency])
    true
  rescue CassandraThrift::NotFoundException
  end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash. Supports the 
  # <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, and <tt>:consistency</tt> 
  # options.
  def get_range(column_family, options = {})
    _, _, options = parse_options([options], READ_DEFAULTS)
    _get_range(column_family, options[:start], options[:finish], options[:count], options[:consistency])
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash. Supports the <tt>:start</tt>, 
  # <tt>:finish</tt>, and <tt>:consistency</tt> options.
  # FIXME will count only MAX_INT records
  def count_range(column_family, options = {})
    get_range(column_family, options.merge(:count => MAX_INT)).size
  end

  # Open a batch operation. Inserts and deletes will be queued until the block
  # closes, and then sent atomically to the server.
  def batch
    @batch = []
    yield
    _compact_mutations
    _dispatch_mutations
    @batch = nil
  end
  
  private
  
  def parse_options(args, defaults)
    args, options = Array(args), defaults    
    if args.last.is_a?(Hash)
      invalid_keys = args.last.keys - options.keys
      if invalid_keys.any?
        this = "#{self.class}##{caller[0].split('`').last[0..-2]}"
        raise ArgumentError, "Invalid options #{invalid_keys.inspect[1..-2]} for #{this}"
      end
      options.merge!(args.pop)
    end    
    [args[0], args[1], options]
  end
  
end
