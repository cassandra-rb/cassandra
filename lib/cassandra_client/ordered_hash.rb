
class CassandraClient
  # Hash is ordered in Ruby 1.9!
  if RUBY_VERSION >= '1.9'
    OrderedHash = ::Hash
  else  
    #  Copyright (c) 2004-2009 David Heinemeier Hansson
    #  
    #  Permission is hereby granted, free of charge, to any person obtaining
    #  a copy of this software and associated documentation files (the
    #  "Software"), to deal in the Software without restriction, including
    #  without limitation the rights to use, copy, modify, merge, publish,
    #  distribute, sublicense, and/or sell copies of the Software, and to
    #  permit persons to whom the Software is furnished to do so, subject to
    #  the following conditions:
    #  
    #  The above copyright notice and this permission notice shall be
    #  included in all copies or substantial portions of the Software.
    #  
    #  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    #  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    #  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    #  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    #  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    #  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    #  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
  
    class OrderedHash < Hash
      require 'enumerator'    
    
      def self.[](*array)
        hash = new
        array.each_slice(2) { |key, value| hash[key] = value }
        hash
      end

      def initialize(*args, &block)
        super
        @keys = []
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

      def inspect
        "#<OrderedHash #{super}>"
      end

    private

      def sync_keys!
        @keys.delete_if {|k| !has_key?(k)}
      end
    end
  end
end
