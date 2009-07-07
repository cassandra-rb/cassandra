class CassandraClient
  include Helper  
  class AccessError < StandardError; end
  
  MAX_INT = 2**31 - 1
  
  attr_reader :keyspace, :host, :port, :quorum, :serializer, :transport, :client, :schema

  # Instantiate a new CassandraClient and open the connection.
  def initialize(keyspace, host = '127.0.0.1', port = 9160, quorum = 1, serializer = CassandraClient::Serialization::JSON)
    @keyspace = keyspace
    @host = host
    @port = port
    @quorum = quorum
    @serializer = serializer

    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
    @transport.open    
    @client = Cassandra::SafeClient.new(
      Cassandra::Client.new(Thrift::BinaryProtocol.new(@transport)), 
      @transport)

    keyspaces = @client.getStringListProperty("tables")
    unless keyspaces.include?(@keyspace)
      raise AccessError, "Keyspace #{@keyspace.inspect} not found. Available: #{keyspaces.inspect}"
    end
        
    @schema = @client.describeTable(@keyspace)
  end
    
  def inspect
    "#<CassandraClient:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @host=#{host.inspect}, @port=#{port}, @quorum=#{quorum}, @serializer=#{serializer.name}>"
  end

  ## Write
  
  # Insert a row for a key. Pass a flat hash for a regular column family, and 
  # a nested hash for a super column family.
  def insert(column_family, key, hash, timestamp = now)
    column_family = column_family.to_s    
    insert = is_super(column_family) ? :insert_super : :insert_standard
    send(insert, column_family, key, hash, timestamp)
  end
  
  private

  def insert_standard(column_family, key, hash, timestamp = now)
    mutation = Batch_mutation_t.new(
      :table => @keyspace, 
      :key => key, 
      :cfmap => {column_family => hash_to_columns(hash, timestamp)})
    @client.batch_insert(mutation, @quorum)
  end 

  def insert_super(column_family, key, hash, timestamp = now)
    mutation = Batch_mutation_super_t.new(
      :table => @keyspace, 
      :key => key, 
      :cfmap => {column_family => hash_to_super_columns(hash, timestamp)})
    @client.batch_insert_superColumn(mutation, @quorum)
  end 
  
  public
  
  ## Delete
  
  # Remove the element at the column_family:key:super_column:column 
  # path you request.
  def remove(column_family, key, super_column = nil, column = nil, timestamp = now)
    column_family = column_family.to_s
    column_family += ":#{super_column}" if super_column
    column_family += ":#{column}" if column
    @client.remove(@keyspace, key, column_family, timestamp, @quorum)
  end
  
  # Remove all rows in the column family you request.
  def clear_column_family!(column_family)
    get_key_range(column_family).each do |key| 
      remove(column_family, key)
    end
  end

  # Remove all rows in the keyspace
  def clear_keyspace!
    @schema.keys.each do |column_family|
      clear_column_family!(column_family)
    end
  end
  
  ## Read

  # Count the elements at the column_family:key:super_column path you 
  # request.
  def count_columns(column_family, key, super_column = nil)
    column_family = column_family.to_s
    column_family += ":#{super_column}" if super_column
    @client.get_column_count(@keyspace, key, column_family)
  end
  
  # Return a list of single values for the elements at the
  # column_family:key:super_column:column path you request.
  def get_columns(column_family, key, super_columns, columns = nil)
    column_family = column_family.to_s
    get_slice_by_names = (is_super(column_family) && !columns) ? :get_slice_super_by_names : :get_slice_by_names
    if super_columns and columns
      column_family += ":#{super_columns}" 
      columns = Array(columns)
    else
      columns = Array(super_columns)
    end
        
    hash = columns_to_hash(@client.send(get_slice_by_names, @keyspace, key, column_family, columns))
    columns.map { |column| hash[column] }
  end
        
  # Return a hash (actually, a CassandraClient::OrderedHash) or a single value 
  # representing the element at the column_family:key:super_column:column 
  # path you request.
  def get(column_family, key, super_column = nil, column = nil, offset = -1, limit = 100)
    column_family = column_family.to_s
    column_family += ":#{super_column}" if super_column
    column_family += ":#{column}" if column          
    
    # You have got to be kidding
    if is_super(column_family)
      if column
        load(@client.get_column(@keyspace, key, column_family).value)
      elsif super_column
        columns_to_hash(@client.get_superColumn(@keyspace, key, column_family).columns)
      else
        columns_to_hash(@client.get_slice_super(@keyspace, key, "#{column_family}:", offset, limit))
      end
    else
      if super_column
        load(@client.get_column(@keyspace, key, column_family).value)
      elsif is_sorted_by_time(column_family)
        result = columns_to_hash(@client.get_columns_since(@keyspace, key, column_family, 0))

        # FIXME Hack until get_slice on a time-sorted column family works again
        result = OrderedHash[*flatten_once(result.to_a[offset, limit])] if offset > -1
        result
      else
        columns_to_hash(@client.get_slice(@keyspace, key, "#{column_family}:", offset, limit))
      end 
    end
  rescue NotFoundException
    is_super(column_family) && !column ? {} : nil
  end
  
  # FIXME
  # def exists?
  # end
  
  # FIXME
  # def get_recent(column_family, key, super_column = nil, column = nil, timestamp = 0)
  # end

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash.
  def get_key_range(column_family, key_range = ''..'', limit = 100)      
    column_families = Array(column_family).map {|c| c.to_s}
    @client.get_key_range(@keyspace, column_families, key_range.begin, key_range.end, limit)
  end
  
  # Count all rows in the column_family you request. Requires the table 
  # to be partitioned with OrderPreservingHash.
  def count(column_family, key_range = ''..'', limit = MAX_INT)
    get_key_range(column_family, key_range, limit).size
  end      
  
  private
    
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
