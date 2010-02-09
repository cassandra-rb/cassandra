class Cassandra
  class Mock
    def initialize(keyspace, servers=nil, options={})
      #read storage-conf.xml
    end

    def clear_keyspace!
      @keyspace = {}
    end

    def insert(column_family, key, hash, options = {})
      if @batch
        @batch << [:insert, column_family, key, hash, options]
      else
        raise ArgumentError if key.nil?
        @keyspace[column_family] ||= OrderedHash.new
        if @keyspace[column_family][key]
          @keyspace[column_family][key] = OrderedHash[@keyspace[column_family][key].merge(hash).sort{|a,b| a[0] <=> b[0]}]
        else
          @keyspace[column_family][key] = OrderedHash[hash.sort{|a,b| a[0] <=> b[0]}]
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
      @keyspace[column_family] ||= OrderedHash.new
      d = @keyspace[column_family][key] || OrderedHash.new
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
      @keyspace[column_family] ||= OrderedHash.new
      if @batch
        @batch << [:remove, column_family, key, column]
      else
        if column
          @keyspace[column_family][key].delete(column)
        else
          @keyspace[column_family].delete(key)
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
      @keyspace[column_family] = OrderedHash.new
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
  end
end