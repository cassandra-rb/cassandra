
class Cassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol #:nodoc:
    private

    def _mutate(mutation_map, consistency_level)
      client.batch_mutate(@keyspace, mutation_map, consistency_level)
    end

    def _remove(key, column_path, timestamp, consistency_level)
      client.remove(@keyspace, key, column_path, timestamp, consistency_level)
    end

    def _count_columns(column_family, key, super_column, consistency)
      client.get_count(@keyspace, key,
        CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => super_column),
        consistency
      )
    end

    # FIXME: add support for start, stop, count functionality
    def _get_columns(column_family, key, columns, sub_columns, consistency)
      result = if is_super(column_family)
        if sub_columns
          columns_to_hash(column_family, client.get_slice(@keyspace, key,
            CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => columns),
            CassandraThrift::SlicePredicate.new(:column_names => sub_columns),
            consistency))
        else
          columns_to_hash(column_family, client.get_slice(@keyspace, key,
            CassandraThrift::ColumnParent.new(:column_family => column_family),
            CassandraThrift::SlicePredicate.new(:column_names => columns),
            consistency))
        end
      else
        columns_to_hash(column_family, client.get_slice(@keyspace, key,
          CassandraThrift::ColumnParent.new(:column_family => column_family),
          CassandraThrift::SlicePredicate.new(:column_names => columns),
          consistency))
      end

      klass = column_name_class(column_family)
      (sub_columns || columns).map { |name| result[klass.new(name)] }
    end

    def _multiget(column_family, keys, column, sub_column, count, start, finish, reversed, consistency)
      # Single values; count and range parameters have no effect
      if is_super(column_family) and sub_column
        column_path = CassandraThrift::ColumnPath.new(:column_family => column_family, :super_column => column, :column => sub_column)
        multi_column_to_hash!(client.multiget(@keyspace, keys, column_path, consistency))
      elsif !is_super(column_family) and column
        column_path = CassandraThrift::ColumnPath.new(:column_family => column_family, :column => column)
        multi_column_to_hash!(client.multiget(@keyspace, keys, column_path, consistency))

      # Slices
      else
        predicate = CassandraThrift::SlicePredicate.new(:slice_range => 
          CassandraThrift::SliceRange.new(
            :reversed => reversed, 
            :count => count, 
            :start => start, 
            :finish => finish))

        if is_super(column_family) and column
          column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => column)
          multi_sub_columns_to_hash!(column_family, client.multiget_slice(@keyspace, keys, column_parent, predicate, consistency))
        else
          column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
          multi_columns_to_hash!(column_family, client.multiget_slice(@keyspace, keys, column_parent, predicate, consistency))
        end
      end
    end

    def _get_range(column_family, start_key, finish_key, key_count, columns, start, finish, count, consistency)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
      predicate = if columns
                    CassandraThrift::SlicePredicate.new(:column_names => columns)
                  else
                    CassandraThrift::SlicePredicate.new(:slice_range => 
                      CassandraThrift::SliceRange.new(
                        :start  => start, 
                        :finish => finish,
                        :count  => count))
                  end
      range = CassandraThrift::KeyRange.new(:start_key => start_key, :end_key => finish_key, :count => key_count)
      client.get_range_slices(@keyspace, column_parent, predicate, range, consistency)
    end
  end
end
