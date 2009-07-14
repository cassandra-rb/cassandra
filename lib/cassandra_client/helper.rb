class CassandraClient
  module Helper

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
        hash[c.name] = if c.is_a?(SuperColumn)
          columns_to_hash(c.columns)
        else
          load(c.value)
        end
      end
      hash
    end  
    
    def hash_to_columns(hash, timestamp)
      hash.map do |column, value|
        Column.new(:name => column, :value => dump(value), :timestamp => timestamp)
      end    
    end
    
    def hash_to_super_columns(hash, timestamp)
      hash.map do |super_column, columns|
        SuperColumn.new(:name => super_column, :columns => hash_to_columns(columns, timestamp))
      end
    end
    
    def time_in_microseconds
      time = Time.now
      time.to_i * 1_000_000 + time.usec
    end
    alias :now :time_in_microseconds
    
    def flatten_once(array)
      result = []
      array.each { |el| result.concat(el) }
      result
    end    
  end
end