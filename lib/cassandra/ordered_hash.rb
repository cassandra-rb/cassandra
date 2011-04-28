# OrderedHash is namespaced to prevent conflicts with other implementations
class Cassandra
    class OrderedHashInt < Hash #:nodoc:
      def initialize(*args, &block)
        super
        @keys = []
      end

      def self.[](*args)
        ordered_hash = new

        if (args.length == 1 && args.first.is_a?(Array))
          args.first.each do |key_value_pair|
            next unless (key_value_pair.is_a?(Array))
            ordered_hash[key_value_pair[0]] = key_value_pair[1]
          end

          return ordered_hash
        end

        unless (args.size % 2 == 0)
          raise ArgumentError.new("odd number of arguments for Hash")
        end

        args.each_with_index do |val, ind|
          next if (ind % 2 != 0)
          ordered_hash[val] = args[ind + 1]
        end

        ordered_hash
      end

      def initialize_copy(other)
        super
        # make a deep copy of keys
        @keys = other.keys
      end

      def []=(key, value)
        @keys << key if !has_key?(key)
        super
      end

      def delete(key)
        if has_key? key
          index = @keys.index(key)
          @keys.delete_at index
        end
        super
      end

      def delete_if
        super
        sync_keys!
        self
      end

      def reject!
        super
        sync_keys!
        self
      end

      def reject(&block)
        dup.reject!(&block)
      end

      def keys
        @keys.dup
      end

      def values
        @keys.collect { |key| self[key] }
      end

      def to_hash
        self
      end

      def to_a
        @keys.map { |key| [ key, self[key] ] }
      end

      def each_key
        @keys.each { |key| yield key }
      end

      def each_value
        @keys.each { |key| yield self[key]}
      end

      def each
        @keys.each {|key| yield [key, self[key]]}
      end

      alias_method :each_pair, :each

      def clear
        super
        @keys.clear
        self
      end

      def shift
        k = @keys.first
        v = delete(k)
        [k, v]
      end

      def merge!(other_hash)
        other_hash.each {|k,v| self[k] = v }
        self
      end

      def merge(other_hash)
        dup.merge!(other_hash)
      end

      # When replacing with another hash, the initial order of our keys must come from the other hash -ordered or not.
      def replace(other)
        super
        @keys = other.keys
        self
      end

    private

      def sync_keys!
        @keys.delete_if {|k| !has_key?(k)}
      end
    end

  class OrderedHash < OrderedHashInt #:nodoc:
    def initialize(*args, &block)
      @timestamps = Hash.new
      super
    end

    def initialize_copy(other)
      @timestamps = other.timestamps
      super
    end

    def []=(key, value, timestamp = nil)
      @timestamps[key] = timestamp
      super(key, value)
    end

    def delete(key)
      @timestamps.delete(key)
      super
    end

    def delete_if(&block)
      @timestamps.delete_if(&block)
      super
    end

    def reject!(&block)
      @timestamps.reject!(&block)
      super
    end

    def timestamps
      @timestamps.dup
    end

    def clear
      @timestamps.clear
      super
    end

    def shift
      k, v = super
      @timestamps.delete(k)
      [k, v]
    end

    def replace(other)
      @timestamps = other.timestamps
      super
    end

    def inspect
      "#<OrderedHash #{super}\n#{@timestamps.inspect}>"
    end
  end
end
