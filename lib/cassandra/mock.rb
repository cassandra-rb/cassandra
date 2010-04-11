require 'nokogiri'

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

    def initialize(keyspace, storage_xml)
      @keyspace = keyspace
      @column_name_class = {}
      @sub_column_name_class = {}
      @storage_xml = storage_xml
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

    def insert(column_family, key, hash_or_array, options = {})
      if @batch
        @batch << [:insert, column_family, key, hash_or_array, options]
      else
        raise ArgumentError if key.nil?
        if column_family_type(column_family) == 'Standard'
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
      if column_family_type(column_family) == 'Standard'
        get_standard(column_family, key, column, options)
      else
        get_super(column_family, key, column, sub_column, options)
      end
    end

    def get_standard(column_family, key, column, options)
      row = cf(column_family)[key] || OrderedHash.new
      if column
        row[column]
      else
        apply_count(row, options[:count], options[:reversed])
      end
    end

    def get_super(column_family, key, column, sub_column, options)
      if column
        if sub_column
          if cf(column_family)[key] &&
             cf(column_family)[key][column] &&
             cf(column_family)[key][column][sub_column]
            cf(column_family)[key][column][sub_column]
          else
            nil
          end
        else
          row = cf(column_family)[key] && cf(column_family)[key][column] ?
            cf(column_family)[key][column] :
            OrderedHash.new
          if options[:start] || options[:finish]
            start  = to_compare_with_type(options[:start],  column_family, false)
            finish = to_compare_with_type(options[:finish], column_family, false)
            ret = OrderedHash.new
            row.keys.each do |key|
              if (start.nil? || key >= start) && (finish.nil? || key <= finish)
                ret[key] = row[key]
              end
            end
            row = ret
          end
          apply_count(row, options[:count], options[:reversed])
        end
      elsif cf(column_family)[key]
        cf(column_family)[key]
      else
        OrderedHash.new
      end
    end

    def exists?(column_family, key, column=nil)
      !!get(column_family, key, column)
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
        @batch << [:remove, column_family, key, column]
      else
        if column
          if sub_column
            cf(column_family)[key][column].delete(sub_column)
          else
            cf(column_family)[key].delete(column)
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

    def count_columns(column_family, key, column=nil)
      get(column_family, key, column).keys.length
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

    def get_range(column_family, options = {})
      column_family, _, _, options = extract_and_validate_params_for_real(column_family, "", [options], READ_DEFAULTS)
      _get_range(column_family, options[:start], options[:finish], options[:count]).keys
    end

    def count_range(column_family, options={})
      count = 0
      l = []
      start_key = ''
      while (l = get_range(column_family, options.merge(:count => 1000, :start => start_key))).size > 0
        count += l.size
        start_key = l.last.succ
      end
      count
    end

    def schema(load=true)
      if !load && !@schema
        []
      else
        @schema ||= schema_for_keyspace(@keyspace)
      end
    end

    private

    def _get_range(column_family, start, finish, count)
      ret = OrderedHash.new
      start  = to_compare_with_type(start,  column_family)
      finish = to_compare_with_type(finish, column_family)
      cf(column_family).keys.sort.each do |key|
        break if ret.keys.size >= count
        if (start.nil? || key >= start) && (finish.nil? || key <= finish)
          ret[key] = cf(column_family)[key]
        end
      end
      ret
    end

    def schema_for_keyspace(keyspace)
      doc = read_storage_xml
      ret = {}
      doc.css("Keyspaces Keyspace[@Name='#{keyspace}']").css('ColumnFamily').each do |cf|
        ret[cf['Name']] = {}
        if cf['CompareSubcolumnsWith']
          ret[cf['Name']]['CompareSubcolumnsWith'] = 'org.apache.cassandra.db.marshal.' + cf['CompareSubcolumnsWith']
        end
        if cf['CompareWith']
          ret[cf['Name']]['CompareWith'] = 'org.apache.cassandra.db.marshal.' + cf['CompareWith']
        end
        if cf['ColumnType'] == 'Super'
          ret[cf['Name']]['Type'] = 'Super'
        else
          ret[cf['Name']]['Type'] = 'Standard'
        end
      end
      ret
    end

    def read_storage_xml
      @doc ||= Nokogiri::XML(open(@storage_xml))
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
        schema[column_family.to_s]["CompareWith"]
      else
        schema[column_family.to_s]["CompareSubcolumnsWith"]
      end

      case klass
      when "org.apache.cassandra.db.marshal.UTF8Type", "org.apache.cassandra.db.marshal.BytesType"
        column_name
      when "org.apache.cassandra.db.marshal.TimeUUIDType"
        SimpleUUID::UUID.new(column_name)
      when "org.apache.cassandra.db.marshal.LongType"
        Long.new(column_name)
      else
        raise "Unknown column family type: #{klass.inspect}"
      end
    end

    def column_family_type(column_family)
      schema[column_family.to_s]['Type']
    end

    def cf(column_family)
      @data[column_family.to_sym] ||= OrderedHash.new
    end

    def merge_and_sort(old_stuff, new_stuff)
      if new_stuff.is_a?(Array)
        new_stuff = new_stuff.inject({}){|h,k| h[k] = nil; h }
      end
      OrderedHash[old_stuff.merge(new_stuff).sort{|a,b| a[0] <=> b[0]}]
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
  end
end
