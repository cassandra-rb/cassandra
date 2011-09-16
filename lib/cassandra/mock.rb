class SimpleUUID::UUID
  def >=(other)
    (self <=> other) >= 0
  end

  def <=(other)
    (self <=> other) <= 0
  end
end

class Cassandra
  class Mock
    include ::Cassandra::Helpers
    include ::Cassandra::Columns

    attr_reader :keyspace

    def initialize(keyspace, schema)
      @is_super = {}
      @keyspace = keyspace
      @column_name_class = {}
      @sub_column_name_class = {}
      @indexes = {}
      @schema = schema[keyspace]
      clear_keyspace!
    end

    def disconnect!
    end

    def clear_keyspace!
      @data = {}
    end

    def clear_column_family!(column_family)
      @data[column_family.to_sym] = OrderedHash.new
    end

    def default_write_consistency=(value)
      WRITE_DEFAULTS[:consistency] = value
    end

    def default_read_consistency=(value)
      READ_DEFAULTS[:consistency] = value
    end

    def insert(column_family, key, hash_or_array, options = {})
      if @batch
        @batch << [:insert, column_family, key, hash_or_array, options]
      else
        raise ArgumentError if key.nil?
        if !is_super(column_family)
          insert_standard(column_family, key, hash_or_array)
        else
          insert_super(column_family, key, hash_or_array)
        end
      end
    end

    def insert_standard(column_family, key, hash_or_array)
      old = cf(column_family)[key] || OrderedHash.new
      cf(column_family)[key] = merge_and_sort(old, hash_or_array)
    end

    def insert_super(column_family, key, hash)
      raise ArgumentError unless hash.is_a?(Hash)
      cf(column_family)[key] ||= OrderedHash.new

      hash.keys.each do |sub_key|
        old = cf(column_family)[key][sub_key] || OrderedHash.new
        cf(column_family)[key][sub_key] = merge_and_sort(old, hash[sub_key])
      end
    end

    def batch
      @batch = []
      yield
      b = @batch
      @batch = nil
      b.each do |mutation|
        send(*mutation)
      end
    ensure
      @batch = nil
    end

    def get(column_family, key, *columns_and_options)
      column_family, column, sub_column, options =
        extract_and_validate_params_for_real(column_family, [key], columns_and_options, READ_DEFAULTS)
      if !is_super(column_family)
        get_standard(column_family, key, column, options)
      else
        get_super(column_family, key, column, sub_column, options)
      end
    end

    def get_standard(column_family, key, column, options)
      columns = cf(column_family)[key] || OrderedHash.new
      row = columns_to_hash(column_family, columns)

      if column
        row[column]
      else
        row = apply_range(row, column_family, options[:start], options[:finish])
        row = apply_count(row, options[:count], options[:reversed])
      end
    end

    def get_super(column_family, key, column, sub_column, options)
      columns = cf(column_family)[key] || OrderedHash.new
      row = columns_to_hash(column_family, columns)

      if column
        if sub_column
          if row[column] &&
            row[column][sub_column]
            row[column][sub_column]
          else
            nil
          end
        else
          row = row[column] || OrderedHash.new
          row = apply_range(row, column_family, options[:start], options[:finish], false)
          row = apply_count(row, options[:count], options[:reversed])
        end
      else
        row
      end
    end

    def exists?(column_family, key, *columns_and_options)
      column_family, column, sub_column, options = extract_and_validate_params_for_real(column_family, [key], columns_and_options, READ_DEFAULTS)
      results = get(column_family, key, column, sub_column)

      ![{}, nil].include?(results)
    end

    def multi_get(column_family, keys, *columns_and_options)
      column_family, column, sub_column, options = extract_and_validate_params_for_real(column_family, keys, columns_and_options, READ_DEFAULTS)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get(column_family, key)
        hash
      end
    end

    def remove(column_family, key, *columns_and_options)
      column_family, column, sub_column, options = extract_and_validate_params_for_real(column_family, key, columns_and_options, WRITE_DEFAULTS)
      if @batch
        @batch << [:remove, column_family, key, column, sub_column]
      else
        if column
          if sub_column
            cf(column_family)[key][column].delete(sub_column.to_s) if cf(column_family)[key][column]
          else
            cf(column_family)[key].delete(column.to_s)  if cf(column_family)[key]
          end
        else
          cf(column_family).delete(key)
        end
      end
    end

    def get_columns(column_family, key, *columns_and_options)
      column_family, columns, sub_columns, options = extract_and_validate_params_for_real(column_family, key, columns_and_options, READ_DEFAULTS)
      d = get(column_family, key)

      if sub_columns
        sub_columns.collect do |sub_column|
          d[columns][sub_column]
        end
      else
        columns.collect do |column|
          d[column]
        end
      end
    end

    def count_columns(column_family, key, *columns_and_options)
      column_family, columns, sub_columns, options = extract_and_validate_params_for_real(column_family, key, columns_and_options, READ_DEFAULTS)

      get(column_family, key, columns, options).keys.length
    end

    def multi_get_columns(column_family, keys, columns)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get_columns(column_family, key, columns)
        hash
      end
    end

    def multi_count_columns(column_family, keys)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = count_columns(column_family, key)
        hash
      end
    end

    def get_range(column_family, options = {}, &blk)
      column_family, _, _, options = extract_and_validate_params_for_real(column_family, "", [options],
                                                                          READ_DEFAULTS.merge(:start_key  => nil,
                                                                                              :finish_key => nil,
                                                                                              :key_count  => 100,
                                                                                              :columns    => nil,
                                                                                              :reversed   => false
                                                                                             )
                                                                         )
      res = _get_range(column_family,
                 options[:start_key],
                 options[:finish_key],
                 options[:key_count],
                 options[:columns],
                 options[:start],
                 options[:finish],
                 options[:count],
                 options[:consistency],
                 options[:reversed], &blk)

      if blk.nil?
        res
      else
        nil
      end
    end

    def get_range_keys(column_family, options = {})
      get_range(column_family,options.merge!(:columns => [])).keys
    end

    def count_range(column_family, options = {})
      Hash[get_range(column_family, options).select{|k,v| v.length > 0}].keys.compact.length
    end

    def each_key(column_family, options = {})
      each(column_family, options.merge!(:columns => [])) do |key, value|
        yield key
      end
    end

    def each(column_family, options = {})
      batch_size    = options.delete(:batch_size) || 100
      count         = options.delete(:key_count)
      yielded_count = 0

      options[:start_key] ||= ''
      last_key  = nil

      while options[:start_key] != last_key && (count.nil? || count > yielded_count)
        options[:start_key] = last_key
        res = get_range(column_family, options.merge!(:start_key => last_key, :key_count => batch_size))
        res.each do |key, columns|
          next if options[:start_key] == key
          next if yielded_count == count
          yield key, columns
          yielded_count += 1
          last_key = key
        end
      end
    end

    def create_index(ks_name, cf_name, c_name, v_class)
      if @indexes[ks_name] &&
        @indexes[ks_name][cf_name] &&
        @indexes[ks_name][cf_name][c_name]
        nil

      else
        @indexes[ks_name] ||= {}
        @indexes[ks_name][cf_name] ||= {}
        @indexes[ks_name][cf_name][c_name] = true
      end
    end

    def drop_index(ks_name, cf_name, c_name)
      if @indexes[ks_name] &&
        @indexes[ks_name][cf_name] &&
        @indexes[ks_name][cf_name][c_name]

        @indexes[ks_name][cf_name].delete(c_name)
      else
        nil
      end
    end

    def create_index_expression(c_name, value, op)
      {:column_name => c_name, :value => value, :comparison => op}
    end
    alias :create_idx_expr :create_index_expression

    def create_index_clause(idx_expressions, start = "", count = 100)
      {:start => start, :index_expressions => idx_expressions, :count => count, :type => :index_clause}
    end
    alias :create_idx_clause :create_index_clause

    def get_indexed_slices(column_family, idx_clause, *columns_and_options)
      column_family, columns, _, options =
        extract_and_validate_params_for_real(column_family, [], columns_and_options, READ_DEFAULTS.merge(:key_count => 100, :key_start => ""))

      unless [Hash, OrderedHash].include?(idx_clause.class) && idx_clause[:type] == :index_clause
        idx_clause = create_index_clause(idx_clause, options[:key_start], options[:key_count])
      end

      ret = {}
      cf(column_family).each do |key, row|
        next if idx_clause[:start] != '' && key < idx_clause[:start]
        next if ret.length == idx_clause[:count]

        matches = []
        idx_clause[:index_expressions].each do |expr|
          next if row[expr[:column_name]].nil?
          next unless row[expr[:column_name]].send(expr[:comparison].to_sym, expr[:value])

          matches << expr
        end

        ret[key] = row if matches.length == idx_clause[:index_expressions].length
      end

      ret
    end

    def add(column_family, key, value, *columns_and_options)
      column_family, column, sub_column, options = extract_and_validate_params_for_real(column_family, key, columns_and_options, WRITE_DEFAULTS)

      if is_super(column_family)
        cf(column_family)[key]                      ||= OrderedHash.new
        cf(column_family)[key][column]              ||= OrderedHash.new
        cf(column_family)[key][column][sub_column]  ||= 0
        cf(column_family)[key][column][sub_column]  += value
      else
        cf(column_family)[key]                      ||= OrderedHash.new
        cf(column_family)[key][column]              ||= 0
        cf(column_family)[key][column]              += value
      end

      nil
    end

    def column_families
      cf_defs = {}
      schema.each do |key, value|
        cf_def = Cassandra::ColumnFamily.new

        value.each do |property, property_value|
          cf_def.send(:"#{property}=", property_value)
        end

        cf_defs[key] = cf_def
      end

      cf_defs
    end

    def schema(load=true)
      @schema
    end

    def column_family_property(column_family, key)
      schema[column_family.to_s][key]
    end

    def add_column_family(cf)
      @schema[cf.name.to_s] ||= OrderedHash.new

      cf.instance_variables.each do |var|
        @schema[cf.name.to_s][var.slice(1..-1)] = cf.instance_variable_get(var)
      end
    end

    def update_column_family(cf)
      return false unless @schema.include?(cf.name.to_s)

      cf.instance_variables.each do |var|
        @schema[cf.name.to_s][var.slice(1..-1)] = cf.instance_variable_get(var)
      end
    end

    def drop_column_family(column_family_name)
      @schema.delete(column_family_name)
    end

    private

    def schema_for_keyspace(keyspace)
      @schema
    end

    def _get_range(column_family, start_key, finish_key, key_count, columns, start, finish, count, consistency, reversed, &blk)
      ret = OrderedHash.new
      start  = to_compare_with_type(start,  column_family)
      finish = to_compare_with_type(finish, column_family)
      cf(column_family).keys.sort.each do |key|
        break if ret.keys.size >= key_count
        if (start_key.nil? || key >= start_key) && (finish_key.nil? || key <= finish_key)
          if columns
            #ret[key] = columns.inject(OrderedHash.new){|hash, column_name| hash[column_name] = cf(column_family)[key][column_name]; hash;}
            ret[key] = columns_to_hash(column_family, cf(column_family)[key].select{|k,v| columns.include?(k)})
            ret[key] = apply_count(ret[key], count, reversed)
            blk.call(key,ret[key]) unless blk.nil?
          else
            #ret[key] = apply_range(cf(column_family)[key], column_family, start, finish, !is_super(column_family))
            start, finish = finish, start if reversed
            ret[key] = apply_range(columns_to_hash(column_family, cf(column_family)[key]), column_family, start, finish)
            ret[key] = apply_count(ret[key], count, reversed)
            blk.call(key,ret[key]) unless blk.nil?
          end
        end
      end
      ret
    end

    def extract_and_validate_params_for_real(column_family, keys, args, options)
      column_family, columns, sub_column, options = extract_and_validate_params(column_family, keys, args, options)
      options[:start] = nil if options[:start] == ''
      options[:finish] = nil if options[:finish] == ''
      [column_family, to_compare_with_types(columns, column_family), to_compare_with_types(sub_column, column_family, false), options]
    end

    def to_compare_with_types(column_names, column_family, standard=true)
      if column_names.is_a?(Array)
        column_names.collect do |name|
          to_compare_with_type(name, column_family, standard)
        end
      else
        to_compare_with_type(column_names, column_family, standard)
      end
    end

    def to_compare_with_type(column_name, column_family, standard=true)
      return column_name if column_name.nil?
      klass = if standard
        column_name_class(column_family)
      else
        sub_column_name_class(column_family)
      end

      klass.new(column_name)
    end

    def cf(column_family)
      @data[column_family.to_sym] ||= OrderedHash.new
    end

    def merge_and_sort(old_stuff, new_stuff)
      if new_stuff.is_a?(Array)
        new_stuff = new_stuff.inject({}){|h,k| h[k] = nil; h }
      end

      new_stuff = new_stuff.to_a.inject({}){|h,k| h[k[0].to_s] = k[1]; h }

      OrderedHash[old_stuff.merge(new_stuff).sort{|a,b| a[0] <=> b[0]}]
    end

    def columns_to_hash(column_family, columns)
      column_class, sub_column_class = column_name_class(column_family), sub_column_name_class(column_family)
      output = OrderedHash.new

      columns.each do |column_name, value|
        column = column_class.new(column_name)

        if [Hash, OrderedHash].include?(value.class)
          output[column] ||= OrderedHash.new
          value.each do |sub_column, sub_column_value|
            output[column][sub_column_class.new(sub_column)] = sub_column_value
          end
        else
          output[column_class.new(column_name)] = value
        end
      end

      output
    end

    def apply_count(row, count, reversed=false)
      if count
        keys = row.keys.sort
        keys = keys.reverse if reversed
        keys = keys[0...count]
        keys.inject(OrderedHash.new) do |memo, key|
          memo[key] = row[key]
          memo
        end
      else
        row
      end
    end

    def apply_range(row, column_family, strt, fin, standard=true)
      start  = to_compare_with_type(strt, column_family, standard)
      finish = to_compare_with_type(fin,  column_family, standard)
      ret = OrderedHash.new
      row.keys.each do |key|
        if (start.nil? || key >= start) && (finish.nil? || key <= finish)
          ret[key] = row[key]
        end
      end
      ret
    end

  end
end
