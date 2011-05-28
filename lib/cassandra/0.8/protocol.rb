
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

    def _count_columns(column_family, key, super_column, consistency)
      client.get_count(key,
        CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => super_column),
        CassandraThrift::SlicePredicate.new(:slice_range =>
                                            CassandraThrift::SliceRange.new(
                                              :start => '',
                                              :finish => ''
                                            )),
        consistency
      )
    end

    # FIXME: add support for start, stop, count functionality
    def _get_columns(column_family, key, columns, sub_columns, consistency, default_value = nil)
      result = if is_super(column_family) && sub_columns
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

      klass = column_name_class(column_family)
      (sub_columns || columns).map { |name| result[klass.new(name)] || default_value }
    end

    def _multiget(column_family, keys, column, sub_column, count, start, finish, reversed, consistency)
      # Single values; count and range parameters have no effect
      if is_super(column_family) and sub_column
        predicate = CassandraThrift::SlicePredicate.new(:column_names => [sub_column])
        column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => column)
        column_hash = multi_sub_columns_to_hash!(column_family, client.multiget_slice(keys, column_parent, predicate, consistency))

        klass = sub_column_name_class(column_family)
        keys.inject({}){|hash, key| hash[key] = column_hash[key][klass.new(sub_column)]; hash}
      elsif !is_super(column_family) and column
        predicate = CassandraThrift::SlicePredicate.new(:column_names => [column])
        column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
        column_hash  = multi_columns_to_hash!(column_family, client.multiget_slice(keys, column_parent, predicate, consistency))

        klass = column_name_class(column_family)
        keys.inject({}){|hash, key| hash[key] = column_hash[key][klass.new(column)]; hash}

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

    def _get_range(column_family, start_key, finish_key, key_count, columns, start, finish, count, consistency)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
      predicate = if columns
                    CassandraThrift::SlicePredicate.new(:column_names => columns)
                  else
                    CassandraThrift::SlicePredicate.new(:slice_range => 
                      CassandraThrift::SliceRange.new(
                        :start => start, 
                        :finish => finish,
                        :count  => count))
                  end
      range = CassandraThrift::KeyRange.new(:start_key => start_key, :end_key => finish_key, :count => key_count)
      client.get_range_slices(column_parent, predicate, range, consistency)
    end

    # TODO: Supercolumn support
    def _get_indexed_slices(column_family, idx_clause, column, count, start, finish, reversed, consistency)
      column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
      if column
        predicate = CassandraThrift::SlicePredicate.new(:column_names => [column])
      else
        predicate = CassandraThrift::SlicePredicate.new(:slice_range =>
          CassandraThrift::SliceRange.new(
            :reversed => reversed,
            :count => count,
            :start => start,
            :finish => finish))
      end
      client.get_indexed_slices(column_parent, idx_clause, predicate, consistency)
    end
  end
end
