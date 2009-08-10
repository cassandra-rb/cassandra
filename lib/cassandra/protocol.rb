
class Cassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol
    private

    def _insert(mutation, consistency)
      case mutation
      when CassandraThrift::BatchMutationSuper then @client.batch_insert_super_column(@keyspace, mutation, consistency)
      when CassandraThrift::BatchMutation then @client.batch_insert(@keyspace, mutation, consistency)
      end
    end

    def _remove(column_family, key, column, sub_column, consistency, timestamp)
      column_path_or_parent = if is_super(column_family)
        CassandraThrift::ColumnPath.new(:column_family => column_family, :super_column => column, :column => sub_column)
      else
        CassandraThrift::ColumnPath.new(:column_family => column_family, :column => column)
      end
      @client.remove(@keyspace, key, column_path_or_parent, timestamp, consistency)
    end

    def _count_columns(column_family, key, super_column, consistency)
      super_column = super_column.to_s if super_column
      @client.get_count(@keyspace, key,
        CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => super_column),
        consistency
      )
    end

    def _get_columns(column_family, key, columns, sub_columns, consistency)
      result = if is_super(column_family)
        if sub_columns
          columns_to_hash(column_family, @client.get_slice(@keyspace, key,
            CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => columns),
            CassandraThrift::SlicePredicate.new(:column_names => sub_columns),
            consistency))
        else
          columns_to_hash(column_family, @client.get_slice(@keyspace, key,
            CassandraThrift::ColumnParent.new(:column_family => column_family),
            CassandraThrift::SlicePredicate.new(:column_names => columns),
            consistency))
        end
      else
        columns_to_hash(column_family, @client.get_slice(@keyspace, key,
          CassandraThrift::ColumnParent.new(:column_family => column_family),
          CassandraThrift::SlicePredicate.new(:column_names => columns),
          consistency))
      end
      sub_columns || columns.map { |name| result[name] }
    end

    def _get(column_family, key, column, sub_column, count, start, finish, reversed, consistency)
      column = column.to_s if column
      sub_column = sub_column.to_s if sub_column
      
      # Single values; count and range parameters have no effect
      if is_super(column_family) and sub_column
        column_path = CassandraThrift::ColumnPath.new(:column_family => column_family, :super_column => column, :column => sub_column)
        @client.get(@keyspace, key, column_path, consistency).column.value
      elsif !is_super(column_family) and column
        column_path = CassandraThrift::ColumnPath.new(:column_family => column_family, :column => column)
        @client.get(@keyspace, key, column_path, consistency).column.value

      # Slices
      else
        # FIXME Comparable types in range are not enforced
        predicate = CassandraThrift::SlicePredicate.new(:slice_range => 
          CassandraThrift::SliceRange.new(
            :is_ascending => !reversed, 
            :count => count, 
            :start => start.to_s, 
            :finish => finish.to_s))
        
        if is_super(column_family) and column
          column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family, :super_column => column)
          sub_columns_to_hash(column_family, @client.get_slice(@keyspace, key, column_parent, predicate, consistency))
        else
          column_parent = CassandraThrift::ColumnParent.new(:column_family => column_family)
          columns_to_hash(column_family, @client.get_slice(@keyspace, key, column_parent, predicate, consistency))
        end
      end
    end

    def _get_range(column_family, start, finish, count, consistency)
      # FIXME Consistency is ignored
      key_range ||= [nil, nil]
      column_family = column_family.to_s
      @client.get_key_range(@keyspace, column_family, start.to_s, finish.to_s, count)
    end

    def _compact_mutations
      compact_batch = []
      mutations = {}

      @batch << nil # Close it
      @batch.each do |m|
        case m
        when Array, nil
          # Flush compacted mutations
          compact_batch.concat(mutations.values.map {|x| x.values}.flatten)
          mutations = {}
          # Insert delete operation
          compact_batch << m
        else # BatchMutation, BatchMutationSuper
          # Do a nested hash merge
          if mutation_class = mutations[m.class]
            if mutation = mutation_class[m.key]
              if columns = mutation.cfmap[m.cfmap.keys.first]
                columns.concat(m.cfmap.values.first)
              else
                mutation.cfmap.merge!(m.cfmap)
              end
            else
              mutation_class[m.key] = m
            end
          else
            mutations[m.class] = {m.key => m}
          end
        end
      end

      @batch = compact_batch
    end

    def _dispatch_mutations
      @batch.each do |args|
        next unless args
        case args.first
        when CassandraThrift::BatchMutationSuper, CassandraThrift::BatchMutation
          _insert(*args)
        else
          _remove(*args)
        end
      end
    end
  end
end
