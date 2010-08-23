class Cassandra
  # A bunch of crap, mostly related to introspecting on column types
  module Columns #:nodoc:
    private

    def _standard_insert_mutation(column_family, column_name, value, timestamp, _=nil)
      CassandraThrift::Mutation.new(
        :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
          :column => CassandraThrift::Column.new(
            :name      => column_name_class(column_family).new(column_name).to_s,
            :value     => value,
            :timestamp => timestamp
          )
        )
      )
    end

    def _super_insert_mutation(column_family, super_column_name, sub_columns, timestamp, _=nil)
      CassandraThrift::Mutation.new(:column_or_supercolumn => 
        CassandraThrift::ColumnOrSuperColumn.new(
          :super_column => CassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(super_column_name).to_s,
            :columns => sub_columns.collect { |sub_column_name, sub_column_value|
              CassandraThrift::Column.new(
                :name      => sub_column_name_class(column_family).new(sub_column_name).to_s,
                :value     => sub_column_value.to_s,
                :timestamp => timestamp
              )
            }
          )
        )
      )
    end
  end
end
