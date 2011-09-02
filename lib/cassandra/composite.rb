
class Cassandra
  class Composite
    def initialize(*parts)
      if parts.length == 1 && parts[0].instance_of?(String)
        @parts = unpack(parts[0])
      else
        @parts = parts
      end
    end

    def [](*args)
      return @parts[*args]
    end

    def pack
      return @parts.map {|part| [part.length].pack('n') + part + "\x00" }.join('')
    end

    def to_s
      return pack
    end

    private
    def unpack(packed_string)
      parts = []
      while packed_string.length > 0
        length = packed_string.slice(0, 2).unpack('n')[0]
        parts << packed_string.slice(2, length)
        packed_string = packed_string.slice(3 + length, packed_string.length)
      end
      return parts
    end
  end
end

