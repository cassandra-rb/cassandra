
class Cassandra
  # A bunch of crap, mostly related to introspecting on column types
  module Columns #:nodoc:
    private
    
    def is_super(column_family)
      @is_super[column_family] ||= column_family_property(column_family, 'Type') == "Super"
    end
    
    def column_name_class(column_family)
      @column_name_class[column_family] ||= column_name_class_for_key(column_family, "CompareWith")
    end
    
    def sub_column_name_class(column_family)
      @sub_column_name_class[column_family] ||= column_name_class_for_key(column_family, "CompareSubcolumnsWith")
    end
    
    def column_name_class_for_key(column_family, comparator_key)
      property = column_family_property(column_family, comparator_key)
      property =~ /.*\.(.*?)$/
      case $1
      when "LongType" then Long
      when "LexicalUUIDType", "TimeUUIDType" then UUID
      else 
        String # UTF8, Ascii, Bytes, anything else
      end
    end

    def column_family_property(column_family, key)
      @schema[column_family][key]
    rescue NoMethodError
      raise AccessError, "Invalid column family \"#{column_family}\""    
    end
    
    def columns_to_hash(column_family, columns)
      columns_to_hash_for_classes(columns, column_name_class(column_family), sub_column_name_class(column_family))
    end
    
    def sub_columns_to_hash(column_family, columns)
      columns_to_hash_for_classes(columns, sub_column_name_class(column_family))
    end
    
    def columns_to_hash_for_classes(columns, column_name_class, sub_column_name_class = nil)
      hash = OrderedHash.new
      Array(columns).each do |c|
        c = c.super_column || c.column if c.is_a?(CassandraThrift::ColumnOrSuperColumn)
        hash[column_name_class.new(c.name)] = case c
        when CassandraThrift::SuperColumn            
          columns_to_hash_for_classes(c.columns, sub_column_name_class) # Pop the class stack, and recurse
        when CassandraThrift::Column
          c.value
        end
      end
      hash    
    end
        
    def hash_to_columns(column_family, hash, timestamp)
      hash.map do |column, value|
        CassandraThrift::ColumnOrSuperColumn.new(:column => 
          CassandraThrift::Column.new(
            :name => column_name_class(column_family).new(column).to_s, 
            :value => value, 
            :timestamp => timestamp))
      end    
    end
    
    def hash_to_super_columns(column_family, hash, timestamp)
      hash.map do |column, sub_hash|
        sub_columns = sub_hash.map do |sub_column, value|
          CassandraThrift::Column.new(
            :name => sub_column_name_class(column_family).new(sub_column).to_s, 
            :value => value, 
            :timestamp => timestamp)
        end    
        CassandraThrift::ColumnOrSuperColumn.new(:super_column => 
          CassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(column).to_s, 
            :columns => sub_columns))
      end
    end    
  end
end
