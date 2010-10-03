
class Cassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol #:nodoc:
    private

    def _mutate(mutation_map, consistency_level)
      client.batch_mutate(mutation_map, consistency_level)
    end

    def _remove(key, column_path, timestamp, consistency_level)
      client.remove(key, column_path, timestamp, consistency_level)
    end

    def _count_columns(column_family, key, super_column, consistency)
      client.get_count(key,
        CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => super_column),
        consistency
      )
    end

    def _get_columns(column_family, key, columns, sub_columns, consistency)
      result = if is_super(column_family)
        if sub_columns
          columns_to_hash(column_family, client.get_slice(key,
            CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => columns),
            CassandraThrift::SlicePredicate.new(:column_names => sub_columns),
            consistency))
        else
          columns_to_hash(column_family, client.get_slice(key,
            CassandraThrift::ColumnParent.new(:column_family => column_family),
            CassandraThrift::SlicePredicate.new(:column_names => columns),
            consistency))
        end
      else
        columns_to_hash(column_family, client.get_slice(key,
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
        multi_column_to_hash!(client.multiget(keys, column_path, consistency))
      elsif !is_super(column_family) and column
        column_path = CassandraThrift::ColumnPath.new(:column_family => column_family, :column => column)
        multi_column_to_hash!(client.multiget(keys, column_path, consistency))

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
          multi_sub_columns_to_hash!(column_family, client.multiget_slice(keys, column_parent, predicate, consistency))
        else
          column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
          multi_columns_to_hash!(column_family, client.multiget_slice(keys, column_parent, predicate, consistency))
        end
      end
    end

    def _get_range(column_family, start, finish, count, consistency)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
      predicate = CassandraThrift::SlicePredicate.new(:slice_range => CassandraThrift::SliceRange.new(:start => '', :finish => ''))
      range = CassandraThrift::KeyRange.new(:start_key => start, :end_key => finish, :count => count)
      client.get_range_slices(column_parent, predicate, range, 1)
    end
    
    def _get_range_keys(column_family, start, finish, count, consistency)
      _get_range(column_family, start, finish, count, consistency).collect{|i| i.key }
    end

    def each_key(column_family)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family.to_s)
      predicate = CassandraThrift::SlicePredicate.new(:column_names => [])
      range = CassandraThrift::KeyRange.new(:start_key => '', :end_key => '')
      client.get_range_slices(column_parent, predicate, range, 1).each{|i| yield i.key }
    end
  end
end
