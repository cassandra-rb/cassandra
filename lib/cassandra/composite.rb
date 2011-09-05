
class Cassandra
  class Composite
    include ::Comparable
    attr_reader :parts
    attr_reader :column_slice

    def initialize(*parts)
      options = {}
      if parts.last.is_a?(Hash)
        options = parts.pop
      end
      @column_slice = options[:slice]

      if parts.length == 1 && parts[0].instance_of?(self.class)
        @column_slice = parts[0].column_slice
        @parts = parts[0].parts
      elsif parts.length == 1 && parts[0].instance_of?(String) && @column_slice.nil? && valid_packed_composite?(parts[0])
        @parts = unpack(parts[0])
      else
        @parts = parts
      end
    end

    def [](*args)
      return @parts[*args]
    end

    def pack
      packed = @parts.map do |part|
        [part.length].pack('n') + part + "\x00"
      end
      if @column_slice
        part = @parts[-1]
        packed[-1] = [part.length].pack('n') + part + slice_end_of_component
      end
      return packed.join('')
    end

    def to_s
      return pack
    end

    def <=>(other)
      return nil if !other.instance_of?(self.class)
      @parts.zip(other.parts).each do |a, b|
        next if a == b
        return -1 if a.nil?
        return 1 if b.nil?
        return -1 if a < b
        return 1 if a > b
      end
      return 0
    end

    def inspect
      return @parts.inspect
    end

    private
    def slice_end_of_component
      return "\x01" if @column_slice == :inclusive
      return "\xFF" if @column_slice == :exclusive
    end

    def unpack(packed_string)
      parts = []
      while packed_string.length > 0
        length = packed_string.slice(0, 2).unpack('n')[0]
        parts << packed_string.slice(2, length)
        packed_string = packed_string.slice(3 + length, packed_string.length)
      end
      return parts
    end

    def valid_packed_composite?(packed_string)
      while packed_string.length > 0
        length = packed_string.slice(0, 2).unpack('n')[0]
        return false if length.nil? || length + 3 > packed_string.length

        end_of_component = packed_string.slice(2 + length, 1)
        return false if end_of_component != "\x00"

        packed_string = packed_string.slice(3 + length, packed_string.length)
      end
      return true
    end
  end
end

