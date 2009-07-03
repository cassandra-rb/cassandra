class CassandraClient
  class Table
    attr_reader :name, :schema, :parent
    
    MAX_INT = 2**31 - 1
  
    def initialize(name, parent)
      @parent = parent
      @client = parent.client
      @block_for = parent.block_for

      @name = name
      @schema = @client.describeTable(@name)
      extend(parent.serialization)
    end
    
    def inspect(full = true)
      string = "#<CassandraClient::Table:#{object_id}, @name=#{name.inspect}"
      string += ", @schema={#{schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')}}, @parent=#{parent.inspect(false)}" if full
      string + ">"
    end
  
    ## Write
    
    # Insert a row for a key. Pass a flat hash for a regular column family, and 
    # a nested hash for a super column family.
    def insert(key, column_family, hash, timestamp = now)
      column_family = column_family.to_s    
      insert = is_super(column_family) ? :insert_super : :insert_standard
      send(insert, key, column_family, hash, timestamp)
    end
    
    private
  
    def insert_standard(key, column_family, hash, timestamp = now)
      mutation = Batch_mutation_t.new(
        :table => @name, 
        :key => key, 
        :cfmap => {column_family => hash_to_columns(hash, timestamp)})
      @client.batch_insert(mutation, @block_for)
    end 
  
    def insert_super(key, column_family, hash, timestamp = now)
      mutation = Batch_mutation_super_t.new(
        :table => @name, 
        :key => key, 
        :cfmap => {column_family => hash_to_super_columns(hash, timestamp)})
      @client.batch_insert_superColumn(mutation, @block_for)
    end 
    
    public
    
    ## Delete
    
    # Remove the element at the column_family:key:super_column:column 
    # path you request.
    def remove(key, column_family, super_column = nil, column = nil, timestamp = now)
      column_family = column_family.to_s
      column_family += ":#{super_column}" if super_column
      column_family += ":#{column}" if column
      @client.remove(@name, key, column_family, timestamp, @block_for )
    end
    
    # Remove all rows in the column family you request.
    def remove_all(column_family)
      get_key_range(column_family).each do |key| 
        remove(key, column_family)
      end
    end
    
    ## Read
  
    # Count the elements at the column_family:key:super_column path you 
    # request.
    def count_columns(key, column_family, super_column = nil)
      column_family = column_family.to_s
      column_family += ":#{super_column}" if super_column
      @client.get_column_count(@name, key, column_family)
    end
    
    # Return a list of single values for the elements at the
    # column_family:key:super_column:column path you request.
    def get_columns(key, column_family, super_columns, columns = nil)
      column_family = column_family.to_s
      get_slice_by_names = (is_super(column_family) && !columns) ? :get_slice_super_by_names : :get_slice_by_names
      if super_columns and columns
        column_family += ":#{super_columns}" 
        columns = Array(columns)
      else
        columns = Array(super_columns)
      end
          
      hash = columns_to_hash(@client.send(get_slice_by_names, @name, key, column_family, columns))
      columns.map { |column| hash[column] }
    end
          
    # Return a hash (actually, a CassandraClient::OrderedHash) or a single value 
    # representing the element at the column_family:key:super_column:column 
    # path you request.
    def get(key, column_family, super_column = nil, column = nil, limit = 100)
      column_family = column_family.to_s
      column_family += ":#{super_column}" if super_column
      column_family += ":#{column}" if column    
      
      # You have got to be kidding
      if is_super(column_family)
        if column
          load(@client.get_column(@name, key, column_family).value)
        elsif super_column
          columns_to_hash(@client.get_superColumn(@name, key, column_family).columns)
        else
          columns_to_hash(@client.get_slice_super(@name, key, "#{column_family}:", -1, limit))
        end
      else
        if super_column
          load(@client.get_column(@name, key, column_family).value)
        elsif is_sorted_by_time(column_family)
          columns_to_hash(@client.get_columns_since(@name, key, column_family, 0))
        else
          columns_to_hash(@client.get_slice(@name, key, "#{column_family}:", -1, limit))
        end 
      end
    rescue NotFoundException
      is_super(column_family) && !column ? {} : nil
    end  
  
    # Return a list of keys in the column_family you request. Requires the
    # table to be partitioned with OrderPreservingHash.
    def get_key_range(key_range, column_family = nil, limit = 100)      
      column_family, key_range = key_range, ''..'' unless column_family
      column_families = Array(column_family).map {|c| c.to_s}
      @client.get_key_range(@name, column_families, key_range.begin, key_range.end, limit)
    end
    
    # Count all rows in the column_family you request. Requires the table 
    # to be partitioned with OrderPreservingHash.
    def count(key_range, column_family = nil, limit = MAX_INT)
      get_key_range(key_range, column_family, limit).size
    end
      
    private
    
    def is_super(column_family)
      column_family_property(column_family, 'type') == 'Super'
    end

    def is_sorted_by_time(column_family)
      column_family_property(column_family, 'sort') == 'Time'
    end
    
    def column_family_property(column_family_or_path, key)
      column_family = column_family_or_path.to_s.split(':').first    
      @schema[column_family][key]
    rescue NoMethodError
      raise AccessError, "Invalid column family \":#{column_family}\""    
    end
    
    def columns_to_hash(columns)
      hash = ::CassandraClient::OrderedHash.new
      Array(columns).each do |c| 
        if c.is_a?(SuperColumn_t)
          hash[c.name] = columns_to_hash(c.columns)
        else
          hash[c.columnName] = load(c.value)
        end
      end
      hash
    end  
    
    def hash_to_columns(hash, timestamp)
      hash.map do |column, value|
        Column_t.new(:columnName => column, :value => dump(value), :timestamp => timestamp)
      end    
    end
    
    def hash_to_super_columns(hash, timestamp)
      hash.map do |super_column, columns|
        SuperColumn_t.new(:name => super_column, :columns => hash_to_columns(columns, timestamp))
      end
    end
    
    def time_in_microseconds
      time = Time.now
      time.to_i * 1_000_000 + time.usec
    end
    alias :now :time_in_microseconds
        
  end
end