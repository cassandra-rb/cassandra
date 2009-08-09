
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
  def insert(column_family, key, hash, consistency = Consistency::WEAK, timestamp = Time.stamp)
    column_family = column_family.to_s
    mutation = if is_super(column_family)
      CassandraThrift::BatchMutationSuper.new(:key => key, :cfmap => {column_family.to_s => hash_to_super_columns(column_family, hash, timestamp)})
    else
      CassandraThrift::BatchMutation.new(:key => key, :cfmap => {column_family.to_s => hash_to_columns(column_family, hash, timestamp)})
    end
    # FIXME Batched operations discard the consistency argument
    @batch ? @batch << mutation : _insert(mutation, consistency)
  end

  ## Delete

  # Remove the element at the column_family:key:[column]:[sub_column]
  # path you request.
  def remove(column_family, key, column = nil, sub_column = nil, consistency = Consistency::WEAK, timestamp = Time.stamp)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)

    column = column.to_s if column
    sub_column = sub_column.to_s if sub_column
    args = [column_family, key, column, sub_column, consistency, timestamp]
    @batch ? @batch << args : _remove(*args)
  end

  # Remove all rows in the column family you request.
  def clear_column_family!(column_family)
    # Does not support consistency argument
    get_range(column_family).each do |key|
      remove(column_family, key)
    end
  end

  # Remove all rows in the keyspace
  def clear_keyspace!
    # Does not support consistency argument
    @schema.keys.each do |column_family|
      clear_column_family!(column_family)
    end
  end

  ## Read

  # Count the elements at the column_family:key:[super_column] path you
  # request.
  def count_columns(column_family, key, super_column = nil, consistency = Consistency::WEAK)
    _count_columns(column_family, key, super_column, consistency)
  end

  # Multi-key version of Cassandra#count_columns.
  def multi_count_columns(column_family, keys, super_column = nil, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key|
      [key, count_columns(column_family, key, super_column)]
    end._flatten_once]
  end

  # Return a list of single values for the elements at the
  # column_family:key:column[s]:[sub_columns] path you request.
  def get_columns(column_family, key, columns, sub_columns = nil, consistency = Consistency::WEAK)
    _get_columns(column_family, key, columns, sub_columns, consistency)
  end

  # Multi-key version of Cassandra#get_columns.
  def multi_get_columns(column_family, keys, columns, sub_columns = nil, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key|
      [key, get_columns(column_family, key, columns, sub_columns, consistency)]
    end._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request.
  def get(column_family, key, column = nil, sub_column = nil, count = 100, column_range = ''..'', reversed = false, consistency = Consistency::WEAK)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    _get(column_family, key, column, sub_column, count, column_range, reversed, consistency)
  rescue CassandraThrift::NotFoundException
    is_super(column_family) && !sub_column ? OrderedHash.new : nil
  end

  # Multi-key version of Cassandra#get.
  def multi_get(column_family, keys, column = nil, sub_column = nil, count = 100, column_range = ''..'', reversed = false, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key|
      [key, get(column_family, key, column, sub_column, count, column_range, reversed, consistency)]
    end._flatten_once]
  end

  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists.
  def exists?(column_family, key, column = nil, sub_column = nil, consistency = Consistency::WEAK)
    column_family = column_family.to_s
    assert_column_name_classes(column_family, column, sub_column)
    _get(column_family, key, column, sub_column, 1, ''..'', false, consistency)
    true
  rescue CassandraThrift::NotFoundException
  end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash.
  def get_range(column_family, key_range = ''..'', count = 100, consistency = Consistency::WEAK)
    _get_range(column_family, key_range, count, consistency)
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash.
  def count_range(column_family, key_range = ''..'', count = MAX_INT, consistency = Consistency::WEAK)
    get_range(column_family, key_range, count, consistency).size
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
end
