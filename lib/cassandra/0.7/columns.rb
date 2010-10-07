
class Cassandra
  # A bunch of crap, mostly related to introspecting on column types
  module Columns #:nodoc:

    def is_super(column_family)
      @is_super[column_family] ||= column_family_property(column_family, 'column_type') == "Super"
    end

    def column_name_class(column_family)
      @column_name_class[column_family] ||= column_name_class_for_key(column_family, "comparator_type")
    end

    def sub_column_name_class(column_family)
      @sub_column_name_class[column_family] ||= column_name_class_for_key(column_family, "subcomparator_type")
    end

    def column_family_property(column_family, key)
      cfdef = schema.cf_defs.find {|cfdef| cfdef.name == column_family }
      unless cfdef
        raise AccessError, "Invalid column family \"#{column_family}\""
      end
      cfdef.send(key)
    end

    private

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
  end
end