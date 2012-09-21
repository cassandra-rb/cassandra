class Cassandra
  module Columns #:nodoc:
    def _standard_counter_mutation(column_family, column_name, value)
      CassandraThrift::Mutation.new(
        :column_or_supercolumn => CassandraThrift::ColumnOrSuperColumn.new(
          :counter_column => CassandraThrift::CounterColumn.new(
            :name      => column_name_class(column_family).new(column_name).to_s,
            :value     => value
          )
        )
      )
    end

    def _super_counter_mutation(column_family, super_column_name, sub_column, value)
      CassandraThrift::Mutation.new(:column_or_supercolumn =>
        CassandraThrift::ColumnOrSuperColumn.new(
          :counter_super_column => CassandraThrift::SuperColumn.new(
            :name => column_name_class(column_family).new(super_column_name).to_s,
            :columns => [CassandraThrift::CounterColumn.new(
              :name      => sub_column_name_class(column_family).new(sub_column).to_s,
              :value     => value
            )]
          )
        )
      )
    end
  end
end
