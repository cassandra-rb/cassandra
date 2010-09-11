
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
      when "LexicalUUIDType", "TimeUUIDType" then SimpleUUID::UUID
      else
        String # UTF8, Ascii, Bytes, anything else
      end
    end

    def column_family_property(column_family, key)
      unless schema[column_family]
        raise AccessError, "Invalid column family \"#{column_family}\""
      end
      schema[column_family][key]
    end

    def multi_column_to_hash!(hash)
      hash.each do |key, column_or_supercolumn|
        hash[key] = (column_or_supercolumn.column.value if column_or_supercolumn.column)
      end
    end

    def multi_columns_to_hash!(column_family, hash)
      hash.each do |key, columns|
        hash[key] = columns_to_hash(column_family, columns)
      end
    end

    def multi_sub_columns_to_hash!(column_family, hash)
      hash.each do |key, sub_columns|
        hash[key] = sub_columns_to_hash(column_family, sub_columns)
      end
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
        case c
        when CassandraThrift::SuperColumn
          hash.[]=(column_name_class.new(c.name), columns_to_hash_for_classes(c.columns, sub_column_name_class)) # Pop the class stack, and recurse
        when CassandraThrift::Column
          hash.[]=(column_name_class.new(c.name), c.value, c.timestamp)
        end
      end
      hash
    end

  end
end
