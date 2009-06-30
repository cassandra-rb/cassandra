require 'rubygems'
require 'thrift'
require 'json'

$LOAD_PATH << "gen-rb"
require 'cassandra'

class CassandraClient
  attr_reader :client, :transport, :schema
  attr_accessor :table, :tables

  # Instantiate a new CassandraClient and open the connection.
  def initialize(table = nil, host = '127.0.0.1', port = 9160, block_for = 1)
    socket = Thrift::Socket.new(host, port)
    @transport = Thrift::BufferedTransport.new(socket)
    protocol = Thrift::BinaryProtocol.new(@transport)    
    @client = Cassandra::Client.new(protocol)    
    
    @transport.open
    @tables = @client.getStringListProperty("tables")
    @table, @block_for = table || tables.first, block_for
    @schema = @client.describeTable(@table)
  end
  
  ## Write
  
  # Insert a row for a key. Pass a flat hash for a regular column family, and 
  # a nested hash for a super column family.
  def insert(column_family, key, hash, timestamp = Time.now.to_i)
    insert = is_super(column_family) ? :insert_super : :insert_standard
    send(insert, column_family, key, hash, timestamp)
  end
  
  private

  def insert_standard(column_family, key, hash, timestamp = Time.now.to_i)
    mutation = Batch_mutation_t.new(
      :table => @table, 
      :key => key, 
      :cfmap => {column_family => hash_to_columns(hash, timestamp)})
    @client.batch_insert(mutation, @block_for)
  end 

  def insert_super(column_family, key, hash, timestamp = Time.now.to_i)
    mutation = Batch_mutation_super_t.new(
      :table => @table, 
      :key => key, 
      :cfmap => {column_family => hash_to_super_columns(hash, timestamp)})
    @client.batch_insert_superColumn(mutation, @block_for)
  end 
  
  public
  
  ## Delete
  
  # Remove the element at the column_family:key:super_column:column 
  # path you request.
  def remove(column_family, key, super_column = nil, column = nil, timestamp = Time.now.to_i)
    column_family += ":#{super_column}" if super_column
    column_family += ":#{column}" if column
    @client.remove(@table, key, column_family, timestamp, @block_for )
  end   
  
  ## Read

  # Count the elements at the column_family:key:super_column path you 
  # request.
  def count(column_family, key, super_column = nil)
    column_family += ":#{super_column}" if super_column
    @client.get_column_count(@table, key, column_family)
  end
 
  # Return a list of single values for the elements at the
  # column_family:key:super_column:column path you request.
  def get_columns(column_family, key, super_columns, columns = nil)
    get_slice_by_names = (is_super(column_family) && !columns) ? :get_slice_super_by_names : :get_slice_by_names
    if super_columns and columns
      column_family += ":#{super_columns}" 
      columns = Array(columns)
    else
      columns = Array(super_columns)
    end
        
    hash = columns_to_hash(@client.send(get_slice_by_names, @table, key, column_family, columns))
    columns.map { |column| hash[column] }
  end
        
  # Return a hash or single value representing the element at the 
  # column_family:key:super_column:column path you request.
  def get(column_family, key, super_column = nil, column = nil, limit = 100)
    column_family += ":#{super_column}" if super_column
    column_family += ":#{column}" if column    
    
    # You have got to be kidding
    if is_super(column_family)
      if column
        @client.get_column(@table, key, column_family).value
      elsif super_column
        columns_to_hash(@client.get_superColumn(@table, key, column_family).columns)
      else
        columns_to_hash(@client.get_slice_super(@table, key, "#{column_family}:", -1, limit))
      end
    else
      if super_column
        @client.get_column(@table, key, column_family).value
      elsif is_sorted_by_time(column_family)
        columns_to_hash(@client.get_columns_since(@table, key, column_family, 0))
      else
        columns_to_hash(@client.get_slice(@table, key, "#{column_family}:", -1, limit))
      end 
    end
  rescue NotFoundException
    is_super(column_family) && !column ? {} : nil
  end  

  # Return a list of keys in the column_family you request. Requires the
  # table to be partitioned with OrderPreservingHash.
  def get_key_range(column_family, key_range = ''..'', limit = 100)
    @client.get_key_range(@table, Array(column_family), key_range.begin, key_range.end, limit)
  end
  
  private
  
  def is_super(column_family)
    @schema[column_family.split(':').first]['type'] == 'Super'
  rescue NoMethodError
    raise "Invalid column_family #{column_family}"
  end
  
  def is_sorted_by_time(column_family)
    @schema[column_family.split(':').first]['sort'] == 'Time'
  end
  
  def columns_to_hash(columns)
    columns = Array(columns)
    Hash[*flatten_once(columns.map do |c| 
      c.is_a?(SuperColumn_t) ? [c.name, columns_to_hash(c.columns)] : [c.columnName, c.value]
    end)]
  end  
  
  def hash_to_columns(hash, timestamp)
    hash.map do |column, value|
      Column_t.new(:columnName => column, :value => value, :timestamp => timestamp)
    end    
  end
  
  def hash_to_super_columns(hash, timestamp)
    hash.map do |super_column, columns|
      SuperColumn_t.new(:name => super_column, :columns => hash_to_columns(columns, timestamp))
    end
  end
    
  def flatten_once(array)
    result = []
    array.each { |el| result.concat(el) }
    result
  end   
end
