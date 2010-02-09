class Cassandra
  class Mock
    def initialize(keyspace, servers=nil, options={})
      #read storage-conf.xml
    end

    def clear_keyspace!
      @keyspace = {}
    end

    def insert(column_family, key, hash, options = {})
      raise ArgumentError if key.nil?
      @keyspace[column_family] ||= OrderedHash.new
      @keyspace[column_family][key] = hash
    end

    def batch
      yield
    end

    def get(column_family, key, column=nil)
      d = @keyspace[column_family][key] || {}
      column ? d[column] : d
    end

    def exists?(column_family, key, column=nil)
      !!get(column_family, key, column)
    end

    def multi_get(column_family, keys)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get(column_family, key) || {}
        hash
      end
    end

    def remove(column_family, key)
      @keyspace[column_family].delete(key)
    end

    def get_columns(column_family, key, columns)
      d = get(column_family, key)
      columns.collect do |column|
        d[column]
      end
    end

    def clear_column_family!(column_family)
      @keyspace[column_family] = {}
    end

    def count_columns(column_family, key)
      get(column_family, key).keys.length
    end

    def multi_get_columns(column_family, keys, columns)
      keys.inject(OrderedHash.new) do |hash, key|
        hash[key] = get_columns(column_family, key, columns) || {}
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