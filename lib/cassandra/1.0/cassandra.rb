class Cassandra

  ## Counters

  # Add a value to the counter in cf:key:super column:column
  def add(column_family, key, value, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    mutation_map = if is_super(column_family)
      {
        key => {
          column_family => [_super_counter_mutation(column_family, column, sub_column, value)]
        }
      }
    else
      {
        key => {
          column_family => [_standard_counter_mutation(column_family, column, value)]
        }
      }
    end

    @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
  end
end
