class CassandraOld
  class DynamicComposite < Composite
    attr_accessor :types

    def initialize(*parts)
      return if parts.empty?

      options = {}
      if parts.last.is_a?(Hash)
        options = parts.pop
      end
      @column_slice = options[:slice]
      raise ArgumentError if @column_slice != nil && ![:before, :after].include?(@column_slice)

      if parts.length == 1 && parts[0].instance_of?(self.class)
        @column_slice = parts[0].column_slice
        @parts = parts[0].parts
        @types = parts[0].types
      elsif parts.length == 1 && parts[0].instance_of?(String) && @column_slice.nil? && try_packed_composite(parts[0])
        @hash = parts[0].hash
      else
        @types, @parts = parts.transpose
      end
    end

    def pack
      packed_parts = @parts.map do |part|
        [part.length].pack('n') + part + "\x00"
      end

      if @column_slice
        part = @parts[-1]
        packed_parts[-1] = [part.length].pack('n') + part + slice_end_of_component
      end

      packed_types = @types.map do |type|
        if type.length == 1
          [0x8000 | type[0].ord].pack('n')
        else
          [type.length].pack('n') + type
        end
      end

      return packed_types.zip(packed_parts).flatten.join('')
    end

    def fast_unpack(packed_string)
      @hash = packed_string.hash

      @types = []
      @parts = []

      offset = 0
      length = nil
      while offset < packed_string.length
        if packed_string[offset].ord & 0x80 != 0
          @types << packed_string[offset+1]
          offset += 2
        else
          length = packed_string.slice(offset, 2).unpack('n')[0]
          offset += 2
          @types << packed_string.slice(offset, length)
          offset += length
        end
        length = packed_string.slice(offset, 2).unpack('n')[0]
        offset += 2
        @parts << packed_string.slice(offset, length)
        offset += length + 1
      end

      @column_slice = :after if packed_string[-1] == "\x01"
      @column_slice = :before if packed_string[-1] == "\xFF"
    end

    private
    def try_packed_composite(packed_string)
      types = []
      parts = []
      end_of_component = nil
      offset = 0

      read_bytes = proc do |length|
        return false if offset + length > packed_string.length
        out = packed_string.slice(offset, length)
        offset += length
        out
      end

      while offset < packed_string.length
        header = read_bytes.call(2).unpack('n')[0]
        is_alias = header & 0x8000 != 0
        if is_alias
          alias_char = (header & 0xFF).chr
          types << alias_char
        else
          length = header
          return false if length.nil? || length + offset > packed_string.length
          type = read_bytes.call(length)
          types << type
        end
        length = read_bytes.call(2).unpack('n')[0]
        return false if length.nil? || length + offset > packed_string.length
        parts << read_bytes.call(length)
        end_of_component = read_bytes.call(1)
        if offset < packed_string.length
          return false if end_of_component != "\x00"
        end
      end
      @column_slice = :after if end_of_component == "\x01"
      @column_slice = :before if end_of_component == "\xFF"
      @types = types
      @parts = parts
      @hash = packed_string.hash

      return true
    end
  end
end
