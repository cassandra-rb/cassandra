require 'nokogiri'
class Cassandra
  class Mock
    include ::Cassandra::Helpers
    include ::Cassandra::Columns

    def initialize(keyspace, servers=nil, options={})
      #read storage-conf.xml
      @keyspace = keyspace
      @column_name_class = {}
      @sub_column_name_class = {}
      @storage_xml = options[:storage_xml]
    end

    def clear_keyspace!
      @data = {}
    end

    def insert(column_family, key, hash, options = {})
      if @batch
        @batch << [:insert, column_family, key, hash, options]
      else
        raise ArgumentError if key.nil?
        @data[column_family] ||= OrderedHash.new
        if @data[column_family][key]
          @data[column_family][key] = OrderedHash[@data[column_family][key].merge(hash).sort{|a,b| a[0] <=> b[0]}]
        else
          @data[column_family][key] = OrderedHash[hash.sort{|a,b| a[0] <=> b[0]}]
        end
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

    def get(column_family, key, column=nil)
      @data[column_family] ||= OrderedHash.new
      d = @data[column_family][key] || OrderedHash.new
      column ? d[column] : d
    end

    def exists?(column_family, key, column=nil)
      !!get(column_family, key, column)
    end

    def multi_get(column_family, keys)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get(column_family, key) || OrderedHash.new
        hash
      end
    end

    def remove(column_family, key, column=nil)
      @data[column_family] ||= OrderedHash.new
      if @batch
        @batch << [:remove, column_family, key, column]
      else
        if column
          @data[column_family][key].delete(column)
        else
          @data[column_family].delete(key)
        end
      end
    end

    def get_columns(column_family, key, columns)
      d = get(column_family, key)
      columns.collect do |column|
        d[column]
      end
    end

    def clear_column_family!(column_family)
      @data[column_family] = OrderedHash.new
    end

    def count_columns(column_family, key)
      get(column_family, key).keys.length
    end

    def multi_get_columns(column_family, keys, columns)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get_columns(column_family, key, columns) || OrderedHash.new
        hash
      end
    end

    def multi_count_columns(column_family, keys)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = count_columns(column_family, key) || 0
        hash
      end
    end

    def get_range(column_family, options = {})
      column_family, _, _, options = 
        extract_and_validate_params(column_family, "", [options], READ_DEFAULTS)
      _get_range(column_family, options[:start].to_s, options[:finish].to_s, options[:count])
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
      p column_family
      p @data
      @data[column_family.to_sym].keys.sort.each do |key|
        break if ret.keys.size >= count
        if key > start && key < finish
          ret[key] = @data[column_family][key]
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
        if cf['ColumnType']
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
  end
end