require "#{File.expand_path(File.dirname(__FILE__))}/../0.7/protocol"

class Cassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol #:nodoc:
    private

    def _remove_counter(key, column_path, consistency_level)
      client.remove_counter(key, column_path, consistency_level)
    end

    def _add(column_family, key, column, sub_column, value, consistency)
      if is_super(column_family)
        column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => column)
        counter_column = CassandraThrift::CounterColumn.new(:name => sub_column, :value => value)
      else
        column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
        counter_column = CassandraThrift::CounterColumn.new(:name => column, :value => value)
      end
      client.add(key, column_parent, counter_column, consistency)
    end
  end
end
