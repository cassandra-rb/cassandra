
class Cassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol #:nodoc:
    private

    def _insert(mutation, consistency)
      case mutation
      when CassandraThrift::BatchMutationSuper then @client.batch_insert_super_column(@database, mutation, consistency)
      when CassandraThrift::BatchMutation then @client.batch_insert(@database, mutation, consistency)
      end
    end

    def _remove(row_set, key, field, sub_field, consistency, timestamp)
      field_path_or_parent = if is_set(row_set)
        CassandraThrift::ColumnPath.new(:column_family => row_set, :super_column => field, :column => sub_field)
      else
        CassandraThrift::ColumnPath.new(:column_family => row_set, :column => field)
      end
      @client.remove(@database, key, field_path_or_parent, timestamp, consistency)
    end

    def _count_fields(row_set, key, super_field, consistency)
      @client.get_count(@database, key,
        CassandraThrift::ColumnParent.new(:column_family => row_set, :super_column => super_field),
        consistency
      )
    end

    def _get_fields(row_set, key, fields, sub_fields, consistency)
      result = if is_set(row_set)
        if sub_fields
          fields_to_hash(row_set, @client.get_slice(@database, key,
            CassandraThrift::ColumnParent.new(:column_family => row_set, :super_column => fields),
            CassandraThrift::SlicePredicate.new(:column_names => sub_fields),
            consistency))
        else
          fields_to_hash(row_set, @client.get_slice(@database, key,
            CassandraThrift::ColumnParent.new(:column_family => row_set),
            CassandraThrift::SlicePredicate.new(:column_names => fields),
            consistency))
        end
      else
        fields_to_hash(row_set, @client.get_slice(@database, key,
          CassandraThrift::ColumnParent.new(:column_family => row_set),
          CassandraThrift::SlicePredicate.new(:column_names => fields),
          consistency))
      end
      sub_fields || fields.map { |name| result[name] }
    end

    def _get(row_set, key, field, sub_field, count, start, finish, reversed, consistency)
      # Single values; count and range parameters have no effect
      if is_set(row_set) and sub_field
        field_path = CassandraThrift::ColumnPath.new(:column_family => row_set, :super_column => field, :column => sub_field)
        @client.get(@database, key, field_path, consistency).column.value
      elsif !is_set(row_set) and field
        field_path = CassandraThrift::ColumnPath.new(:column_family => row_set, :column => field)
        @client.get(@database, key, field_path, consistency).column.value

      # Slices
      else
        # FIXME Comparable types in range are not enforced
        predicate = CassandraThrift::SlicePredicate.new(:slice_range => 
          CassandraThrift::SliceRange.new(
            :is_ascending => !reversed, 
            :count => count, 
            :start => start.to_s, 
            :finish => finish.to_s))
        
        if is_set(row_set) and field
          field_parent = CassandraThrift::ColumnParent.new(:column_family => row_set, :super_column => field)
          sub_fields_to_hash(row_set, @client.get_slice(@database, key, field_parent, predicate, consistency))
        else
          field_parent = CassandraThrift::ColumnParent.new(:column_family => row_set)
          fields_to_hash(row_set, @client.get_slice(@database, key, field_parent, predicate, consistency))
        end
      end
    end

    def _get_range(row_set, start, finish, count, consistency)
      # FIXME Consistency is ignored
      @client.get_key_range(@database, row_set, start.to_s, finish.to_s, count)
    end
  end
end
