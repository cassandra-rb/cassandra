
class Cassandra
  class Composite
    include ::Comparable
    attr_accessor :parts
    def initialize(*parts)
      if parts.length == 1 && parts[0].instance_of?(self.class)
        @parts = parts[0].parts
      elsif parts.length == 1 && parts[0].instance_of?(String)
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

