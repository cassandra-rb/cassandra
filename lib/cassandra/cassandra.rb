
class Cassandra
  include Columns
  include Protocol

  class AccessError < StandardError; end

  MAX_INT = 2**31 - 1

  module Consistency
    include CassandraThrift::ConsistencyLevel
    NONE = ZERO
    WEAK = ONE
    STRONG = QUORUM
    PERFECT = ALL
  end

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
  # a nested hash for a super column family.
  #
  # options:
  # consistency: request consistency level. default Consistency::WEAK
  # timestamp: transaction timestamp. WARNING CHANGE ONLY IF REALY KNOW THAT ARE YOU DOING. Cassandra use clients timestamps to resolve conflicts
  def insert(column_family, key, hash, options = {})
    options = merge_default_consistency(options)
    options[:timestamp] ||= Time.stamp

    column_family = column_family.to_s
    mutation = if is_super(column_family)
      CassandraThrift::BatchMutationSuper.new(:key => key, :cfmap => {column_family.to_s => hash_to_super_columns(column_family, hash, options[:timestamp])})
    else
      CassandraThrift::BatchMutation.new(:key => key, :cfmap => {column_family.to_s => hash_to_columns(column_family, hash, options[:timestamp])})
    end
    # FIXME Batched operations discard the consistency argument
    @batch ? @batch << mutation : _insert(mutation,  options[:consistency])
  end

  ## Delete

  # Remove the element at the column_family:key:[column]:[sub_column]
  # path you request.
  # options:
  # consistency: request consistency level. default Consistency::WEAK
  # timestamp: transaction timestamp. WARNING CHANGE ONLY IF REALY KNOW THAT ARE YOU DOING. Cassandra use clients timestamps to resolve conflicts
  def remove(column_family, key, column = nil, sub_column = nil, options ={})
    options = merge_default_consistency(options)
    options[:timestamp] ||= Time.stamp

    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)

    column = column.to_s if column
    sub_column = sub_column.to_s if sub_column
    args = [column_family, key, column, sub_column, options[:consistency], options[:timestamp]]
    @batch ? @batch << args : _remove(*args)
  end

  # Remove all rows in the column family you request.
  def clear_column_family!(column_family)
    # Does not support consistency argument
    # FIXME this will delete only MAX_INT records
    get_range(column_family).each do |key|
      remove(column_family, key, nil, nil, :count => MAX_INT)
    end
  end

  # Remove all rows in the keyspace
  def clear_keyspace!
    # Does not support consistency argument
    # FIXME this will delete only MAX_INT records in each column_family
    @schema.keys.each do |column_family|
      clear_column_family!(column_family)
    end
  end

  ## Read

  # Count the elements at the column_family:key:[super_column] path you
  # request.
  # options:
  # count: number of records to return. default 100
  # column_range: range of columns to return. default nil
  # reversed: do reversed search. default false
  # consistency: request consistency level. default Consistency::WEAK
  def count_columns(column_family, key, super_column = nil, options={})
    options = merge_default_consistency(options)

    _count_columns(column_family, key, super_column, options[:consistency])
  end

  # Multi-key version of Cassandra#count_columns.
  # options:
  # see count_columns
  def multi_count_columns(column_family, keys, super_column = nil, options={})
    OrderedHash[*keys.map do |key|
      [key, count_columns(column_family, key, super_column, options)]
    end._flatten_once]
  end

  # Return a list of single values for the elements at the
  # column_family:key:column[s]:[sub_columns] path you request.
  # options:
  # consistency: request consistency level. default Consistency::WEAK
  def get_columns(column_family, key, columns, sub_columns = nil, options={})
    options = merge_default_consistency(options)
    _get_columns(column_family, key, columns, sub_columns, options[:consistency])
  end

  # Multi-key version of Cassandra#get_columns.
  # options:
  # see get_columns
  def multi_get_columns(column_family, keys, columns, sub_columns = nil, options={})
    OrderedHash[*keys.map do |key|
      [key, get_columns(column_family, key, columns, sub_columns, options)]
    end._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request.
  # options:
  # count: number of records to return. default 100
  # column_range: range of columns to return. default nil
  # reversed: do reversed search. default false
  # consistency: request consistency level. default Consistency::WEAK
  def get(column_family, key, column = nil, sub_column = nil, options={})
    options = merge_default_get_options(options)

    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    _get(column_family, key, column, sub_column, options[:count], options[:column_range], options[:reversed], options[:consistency])
  rescue CassandraThrift::NotFoundException
    is_super(column_family) && !sub_column ? OrderedHash.new : nil
  end

  # Multi-key version of Cassandra#get.
  # options:
  # see get
  def multi_get(column_family, keys, column = nil, sub_column = nil, options={})
    OrderedHash[*keys.map do |key|
      [key, get(column_family, key, column, sub_column, options)]
    end._flatten_once]
  end

  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists.
  # options:
  # consistency: request consistency level. default Consistency::WEAK
  def exists?(column_family, key, column = nil, sub_column = nil, options={})
    options = merge_default_consistency(options)

    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    _get(column_family, key, column, sub_column, 1, nil, false, options[:consistency])
    true
  rescue CassandraThrift::NotFoundException
  end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash.
  # options:
  # count: number of records to return. default 100
  # consistency: request consistency level. default Consistency::WEAK
  def get_range(column_family, key_range = nil, options={})
    options = merge_default_consistency(options)
    options[:count] ||= 100

    _get_range(column_family, key_range, options[:count], options[:consistency])
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash.
  # options:
  # count: number of records to return. default 100
  def count_range(column_family, key_range = nil, options={:count => MAX_INT})
    #FIXME will count only MAX_INT records
    get_range(column_family, key_range, options).size
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
  def merge_default_consistency(options)
    {:consistency => Consistency::WEAK}.merge(options)
  end

  def merge_default_get_options(options)
    merge_default_consistency({ :count => 100, :column_range => nil, :reversed => false }.merge(options))
  end

end
