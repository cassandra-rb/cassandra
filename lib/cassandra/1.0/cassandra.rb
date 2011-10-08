class Cassandra

  ## Counters

  # Add a value to the counter in cf:key:super column:column
  def add(column_family, key, value, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)
    _add(column_family, key, column, sub_column, value, options[:consistency])
  end
end
