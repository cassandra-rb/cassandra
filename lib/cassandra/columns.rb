
class Cassandra
  # A bunch of crap, mostly related to introspecting on column types
  module Columns #:nodoc:
    private

    def is_super(column_family)
      @is_super[column_family] ||= column_family_property(column_family, 'column_type') == "Super"
    end

    def column_name_class(column_family)
      @column_name_class[column_family] ||= column_name_class_for_key(column_family, "comparator_type")
    end

    def sub_column_name_class(column_family)
      @sub_column_name_class[column_family] ||= column_name_class_for_key(column_family, "subcomparator_type")
    end

    def column_name_class_for_key(column_family, comparator_key)
      property = column_family_property(column_family, comparator_key)
      property =~ /[^(]*\.(.*?)$/
      case $1
      when "LongType" then Long
      when "LexicalUUIDType", "TimeUUIDType" then SimpleUUID::UUID
      when /^CompositeType\(/ then Composite
      else
        String # UTF8, Ascii, Bytes, anything else
      end
    end

    def column_family_property(column_family, key)
      cfdef = schema.cf_defs.find {|cfdef| cfdef.name == column_family }
      unless cfdef
        raise AccessError, "Invalid column family \"#{column_family}\""
      end
      cfdef.send(key)
    end

    def multi_key_slices_to_hash(column_family, array, return_empty_rows = false)
      ret = OrderedHash.new
      array.each do |value|
        next if return_empty_rows == false && value.columns.length == 0
        ret[value.key] = columns_to_hash(column_family, value.columns)
      end
      ret
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
        c = c.super_column || c.column || c.counter_column if c.is_a?(CassandraThrift::ColumnOrSuperColumn)
        case c
        when CassandraThrift::SuperColumn
          hash.[]=(column_name_class.new(c.name), columns_to_hash_for_classes(c.columns, sub_column_name_class)) # Pop the class stack, and recurse
        when CassandraThrift::Column
          hash.[]=(column_name_class.new(c.name), c.value, c.timestamp)
        when CassandraThrift::CounterColumn
          hash.[]=(column_name_class.new(c.name), c.value, 0)
        end
      end
      hash
    end

    def _standard_insert_mutation(column_family, column_name, value, timestamp, ttl = nil)
      CassandraThrift::Mutation.new(
        :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
          :column => CassandraThrift::Column.new(
            :name      => column_name_class(column_family).new(column_name).to_s,
            :value     => value,
            :timestamp => timestamp,
            :ttl       => ttl
          )
        )
      )
    end

    def _super_insert_mutation(column_family, super_column_name, sub_columns, timestamp, ttl = nil)
      CassandraThrift::Mutation.new(:column_or_supercolumn => 
        CassandraThrift::ColumnOrSuperColumn.new(
          :super_column => CassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(super_column_name).to_s,
            :columns => sub_columns.collect { |sub_column_name, sub_column_value|
              CassandraThrift::Column.new(
                :name      => sub_column_name_class(column_family).new(sub_column_name).to_s,
                :value     => sub_column_value.to_s,
                :timestamp => timestamp,
                :ttl       => ttl
              )
            }
          )
        )
      )
    end

    # General info about a deletion object within a mutation
    # timestamp - required. If this is the only param, it will cause deletion of the whole key at that TS
    # supercolumn - opt. If passed, the deletes will only occur within that supercolumn (only subcolumns 
    #               will be deleted). Otherwise the normal columns will be deleted.
    # predicate - opt. Defines how to match the columns to delete. if supercolumn passed, the slice will 
    #               be scoped to subcolumns of that supercolumn.
    
    # Deletes a single column from the containing key/CF (and possibly supercolumn), at a given timestamp. 
    # Although mutations (as opposed to 'remove' calls) support deleting slices and lists of columns in one shot, this is not implemented here.
    # The main reason being that the batch function takes removes, but removes don't have that capability...so we'd need to change the remove
    # methods to use delete mutation calls...although that might have performance implications. We'll leave that refactoring for later.
    def _delete_mutation(cf, column, subcolumn, timestamp, options={})
      deletion_hash = {:timestamp => timestamp}
      if is_super(cf)
        deletion_hash[:super_column] = column if column
        deletion_hash[:predicate] = CassandraThrift::SlicePredicate.new(:column_names => [subcolumn]) if subcolumn
      else
        deletion_hash[:predicate] = CassandraThrift::SlicePredicate.new(:column_names => [column]) if column
      end
      CassandraThrift::Mutation.new(
        :deletion => CassandraThrift::Deletion.new(deletion_hash)
      )
    end
  end
end
