
class CassandraClient
  include Helper  
  class AccessError < StandardError; end
  
  MAX_INT = 2**31 - 1
  
  module Consistency
    include ::ConsistencyLevel
    NONE = ZERO
    WEAK = ONE
    STRONG = QUORUM
    PERFECT = ALL
  end
  
  attr_reader :keyspace, :host, :port, :serializer, :transport, :client, :schema

  # Instantiate a new CassandraClient and open the connection.
  def initialize(keyspace, host = '127.0.0.1', port = 9160, serializer = CassandraClient::Serialization::JSON)
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}

    @keyspace = keyspace
    @host = host
    @port = port
    @serializer = serializer

    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
    @transport.open    
    @client = Cassandra::SafeClient.new(
      Cassandra::Client.new(Thrift::BinaryProtocol.new(@transport)), 
      @transport)

    keyspaces = @client.get_string_list_property("tables")
    unless keyspaces.include?(@keyspace)
      raise AccessError, "Keyspace #{@keyspace.inspect} not found. Available: #{keyspaces.inspect}"
    end
        
    @schema = @client.describe_table(@keyspace)    
  end
    
  def inspect
    "#<CassandraClient:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @host=#{host.inspect}, @port=#{port}, @serializer=#{serializer.name}>"
  end

  ## Write
  
  # Insert a row for a key. Pass a flat hash for a regular column family, and 
  # a nested hash for a super column family.
  def insert(column_family, key, hash, consistency = Consistency::WEAK, timestamp = Time.stamp)
    column_family = column_family.to_s
    mutation = if is_super(column_family) 
      BatchMutationSuper.new(:key => key, :cfmap => {column_family.to_s => hash_to_super_columns(hash, timestamp)})
    else
      BatchMutation.new(:key => key, :cfmap => {column_family.to_s => hash_to_columns(hash, timestamp)})      
    end
    # FIXME Batched operations discard the consistency argument
    @batch ? @batch << mutation : _insert(mutation, consistency)
  end
  
  private
  
  def _insert(mutation, consistency = Consistency::WEAK)
    case mutation
    when BatchMutationSuper then @client.batch_insert_super_column(@keyspace, mutation, consistency)    
    when BatchMutation then @client.batch_insert(@keyspace, mutation, consistency)
    end
  end  
  
  public
  
  ## Delete
  
  # Remove the element at the column_family:key:super_column:column 
  # path you request.
  def remove(column_family, key, super_column = nil, column = nil, consistency = Consistency::WEAK, timestamp = Time.stamp)
    column_family = column_family.to_s
    args = [column_family, key, super_column, column, consistency, timestamp]
    @batch ? @batch << args : _remove(*args)
  end
  
  private 
  
  def _remove(column_family, key, super_column, column, consistency, timestamp)
     super_column, column = column, super_column unless is_super(column_family)
    @client.remove(@keyspace, key,
      ColumnPathOrParent.new(:column_family => column_family, :super_column => super_column, :column => column), 
      timestamp, consistency)
  end
   
  public
  
  # Remove all rows in the column family you request.
  def clear_column_family!(column_family)
    # Does not support consistency argument
    get_key_range(column_family).each do |key| 
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

  # Count the elements at the column_family:key:super_column path you 
  # request.
  def count_columns(column_family, key, super_column = nil, consistency = Consistency::WEAK)
    column_family = column_family.to_s
    super_column = super_column.to_s if super_column
    @client.get_column_count(@keyspace, key, 
      ColumnParent.new(:column_family => column_family, :super_column => super_column),
      consistency
    )
  end
  
  # Multi-key version of CassandraClient#count_columns.
  def multi_count_columns(column_family, keys, super_column = nil, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key|   
      [key, count_columns(column_family, key, super_column)]
    end._flatten_once]
  end  
  
  # Return a list of single values for the elements at the
  # column_family:key:super_column:column path you request.
  def get_columns(column_family, key, super_columns, columns = nil, consistency = Consistency::WEAK)
    column_family = column_family.to_s
    super_columns, columns = columns, super_columns unless columns
    result = if is_super(column_family) && !super_columns 
      columns_to_hash(column_family, @client.get_slice_super_by_names(@keyspace, key, column_family, columns, consistency))
    else
      columns_to_hash(column_family, @client.get_slice_by_names(@keyspace, key, 
        ColumnParent.new(:column_family => column_family, :super_column => super_columns), 
        columns, consistency))
    end    
    columns.map { |name| result[name] }
  end

  # Multi-key version of CassandraClient#get_columns.
  def multi_get_columns(column_family, keys, super_columns, columns = nil, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key| 
      [key, get_columns(column_family, key, super_columns, columns, consistency)]
    end._flatten_once]
  end
        
  # Return a hash (actually, a CassandraClient::OrderedHash) or a single value 
  # representing the element at the column_family:key:super_column:column 
  # path you request.
  def get(column_family, key, super_column = nil, column = nil, limit = 100, consistency = Consistency::WEAK)
    column_family = column_family.to_s
    super_column = super_column.to_s if super_column
    column = column.to_s if column

    # You have got to be kidding
    if is_super(column_family)
      if column
        # FIXME raise if limit applied
        load(@client.get_column(@keyspace, key,  
            ColumnPath.new(:column_family => column_family, :super_column => super_column, :column => column),
            consistency).value)
      elsif super_column
        # FIXME fake limit
        sub_columns_to_hash(column_family, 
          @client.get_super_column(@keyspace, key, 
            SuperColumnPath.new(:column_family => column_family, :super_column => super_column), 
            consistency).columns[0, limit])
      else
        # FIXME add token support
        columns_to_hash(column_family, @client.get_slice_super(@keyspace, key, column_family, '', '', -1, limit, consistency))
      end
    else
      if super_column
        # FIXME raise if limit applied
        load(@client.get_column(@keyspace, key, 
          ColumnPath.new(:column_family => column_family, :column => super_column),
          consistency).value)
      else
        columns_to_hash(column_family, 
          @client.get_slice(@keyspace, key, 
            ColumnParent.new(:column_family => column_family),
            '', '', -1, limit, consistency))
      end 
    end
  rescue NotFoundException
    is_super(column_family) && !column ? OrderedHash.new : nil
  end
  
  # Multi-key version of CassandraClient#get.
  def multi_get(column_family, keys, super_column = nil, column = nil, limit = 100, consistency = Consistency::WEAK)
    OrderedHash[*keys.map do |key| 
      [key, get(column_family, key, super_column, column, limit, consistency)]
    end._flatten_once]
  end
  
  # FIXME
  # def exists?
  # end
  
  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash.
  def get_key_range(column_family, key_range = ''..'', limit = 100, consistency = Consistency::WEAK)      
    column_family = column_family.to_s
    @client.get_key_range(@keyspace, column_family, key_range.begin, key_range.end, limit)
  end
  
  # Count all rows in the column_family you request. Requires the table 
  # to be partitioned with OrderPreservingHash.
  def count(column_family, key_range = ''..'', limit = MAX_INT, consistency = Consistency::WEAK)
    get_key_range(column_family, key_range, limit, consistency).size
  end
  
  def batch
    @batch = []
    yield    
    compact_mutations!
    dispatch_mutations!    
    @batch = nil
  end
  
  private

  def compact_mutations!
    compact_batch = []
    mutations = {}   

    @batch << nil # Close it
    @batch.each do |m|
      case m
      when Array, nil
        # Flush compacted mutations
        compact_batch.concat(mutations.values.map {|x| x.values}.flatten)
        mutations = {}
        # Insert delete operation
        compact_batch << m 
      else # BatchMutation, BatchMutationSuper
        # Do a nested hash merge
        if mutation_class = mutations[m.class]
          if mutation = mutation_class[m.key]
            if columns = mutation.cfmap[m.cfmap.keys.first]
              columns.concat(m.cfmap.values.first)
            else
              mutation.cfmap.merge!(m.cfmap)
            end
          else
            mutation_class[m.key] = m
          end
        else
          mutations[m.class] = {m.key => m}
        end
      end
    end
            
    @batch = compact_batch
  end
  
  def dispatch_mutations!
    @batch.each do |args| 
      case args
      when Array 
        _remove(*args)
      when BatchMutationSuper, BatchMutation 
        _insert(*args)
      end
    end
  end  
  
  def dump(object)
    # Special-case nil as the empty byte array
    return "" if object == nil
    @serializer.dump(object)
  end
  
  def load(object)
    return nil if object == ""  
    @serializer.load(object)
  end  
end
