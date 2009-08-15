
class Cassandra
  # A bunch of crap, mostly related to introspecting on field types
  module Fields #:nodoc:
    private
    
    def is_set(row_set)
      @is_set[row_set] ||= row_set_property(row_set, 'Type') == "Super"
    end
    
    def field_class(row_set)
      @field_class[row_set] ||= field_class_for_key(row_set, "CompareWith")
    end
    
    def sub_field_class(row_set)
      @sub_field_class[row_set] ||= field_class_for_key(row_set, "CompareSubcolumnsWith")
    end
    
    def field_class_for_key(row_set, comparator_key)
      property = row_set_property(row_set, comparator_key)
      property =~ /.*\.(.*?)$/
      case $1
      when "LongType" then Long
      when "LexicalUUIDType", "TimeUUIDType" then UUID
      else 
        String # UTF8, Ascii, Bytes, anything else
      end
    end

    def row_set_property(row_set, key)
      @schema[row_set][key]
    rescue NoMethodError
      raise AccessError, "Invalid row set \"#{row_set}\""    
    end
    
    def assert_field_classes(row_set, fields, sub_fields = nil)      
      {Array(fields) => field_class(row_set), 
        Array(sub_fields) => sub_field_class(row_set)}.each do |fields, klass|
        fields.each { |field| raise Comparable::TypeError, "Expected #{field.inspect} to be a #{klass}" if !field.is_a?(klass) }
      end
    end
    
    def fields_to_hash(row_set, fields)
      fields_to_hash_for_classes(fields, field_class(row_set), sub_field_class(row_set))
    end
    
    def sub_fields_to_hash(row_set, fields)
      fields_to_hash_for_classes(fields, sub_field_class(row_set))
    end
    
    def fields_to_hash_for_classes(fields, field_class, sub_field_class = nil)
      hash = OrderedHash.new
      Array(fields).each do |c|
        c = c.super_column || c.column if c.is_a?(CassandraThrift::ColumnOrSuperColumn)
        hash[field_class.new(c.name)] = case c
          when CassandraThrift::SuperColumn            
            fields_to_hash_for_classes(c.columns, sub_field_class) # Pop the class stack, and recurse
          when CassandraThrift::Column
            c.value
        end
      end
      hash    
    end
    
    def hash_to_fields(row_set, hash, timestamp)
      assert_field_classes(row_set, hash.keys)
      hash_to_fields_without_assertion(row_set, hash, timestamp)
    end
    
    def hash_to_fields_without_assertion(row_set, hash, timestamp)
      hash.map do |field, value|
        CassandraThrift::Column.new(:name => field.to_s, :value => value, :timestamp => timestamp)
      end    
    end    
    
    def hash_to_super_fields(row_set, hash, timestamp)
      assert_field_classes(row_set, hash.keys)      
      hash.map do |field, sub_hash|
        assert_field_classes(row_set, nil, sub_hash.keys)
        CassandraThrift::SuperColumn.new(:name => field.to_s, :columns => hash_to_fields_without_assertion(row_set, sub_hash, timestamp))
      end
    end    
  end
end
