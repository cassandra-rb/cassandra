class CassandraClient
  module Helper
  
    private
    
    def is_super(column_family)
      column_family_property(column_family.to_s, 'Type') == "Super"
    end

    def column_family_property(column_family, key)
      @schema[column_family.to_s][key]
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
        Column.new(:name => column.to_s, :value => dump(value), :timestamp => timestamp)
      end    
    end
    
    def hash_to_super_columns(hash, timestamp)
      hash.map do |super_column, columns|
        SuperColumn.new(:name => super_column.to_s, :columns => hash_to_columns(columns, timestamp))
      end
    end    
  end
end
