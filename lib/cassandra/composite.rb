
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
      raise ArgumentError if @column_slice != nil && ![:before, :after].include?(@column_slice)

      if parts.length == 1 && parts[0].instance_of?(self.class)
        @column_slice = parts[0].column_slice
        @parts = parts[0].parts
      elsif parts.length == 1 && parts[0].instance_of?(String) && @column_slice.nil? && valid_packed_composite?(parts[0])
        unpack(parts[0])
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
      if !other.instance_of?(self.class)
        return @parts.first <=> other
      end
      eoc = slice_end_of_component.unpack('c')[0]
      other_eoc = other.slice_end_of_component.unpack('c')[0]
      @parts.zip(other.parts).each do |a, b|
        next if a == b
        if a.nil? && b.nil?
          return eoc <=> other_eoc
        end

        if a.nil?
          return @column_slice == :after ? 1 : -1
        end
        if b.nil?
          return other.column_slice == :after ? -1 : 1
        end
        return -1 if a < b
        return 1 if a > b
      end
      return 0
    end

    def inspect
      return "#<Composite:#{@column_slice} #{@parts.inspect}>"
    end

    def slice_end_of_component
      return "\x01" if @column_slice == :after
      return "\xFF" if @column_slice == :before
      return "\x00"
    end

    private
    def unpack(packed_string)
      parts = []
      end_of_component = nil
      while packed_string.length > 0
        length = packed_string.slice(0, 2).unpack('n')[0]
        parts << packed_string.slice(2, length)
        end_of_component = packed_string.slice(2 + length, 1)

        packed_string = packed_string.slice(3 + length, packed_string.length)
      end
      @column_slice = :after if end_of_component == "\x01"
      @column_slice = :before if end_of_component == "\xFF"
      @parts = parts
    end

    def valid_packed_composite?(packed_string)
      while packed_string.length > 0
        length = packed_string.slice(0, 2).unpack('n')[0]
        return false if length.nil? || length + 3 > packed_string.length

        end_of_component = packed_string.slice(2 + length, 1)
        if length + 3 != packed_string.length
          return false if end_of_component != "\x00"
        end

        packed_string = packed_string.slice(3 + length, packed_string.length)
      end
      return true
    end

    def hash
      return to_s.hash
    end

    def eql?(other)
      return to_s == other.to_s
    end
  end
end
