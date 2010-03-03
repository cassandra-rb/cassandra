
class Cassandra
  # A temporally-ordered Long class for use in Cassandra column names
  class Long < Comparable

    # FIXME Should unify with or subclass Cassandra::UUID
    def initialize(bytes = nil)
      case bytes
      when self.class # Long
        @bytes = bytes.to_s
      when String
        case bytes.size
        when 8 # Raw byte array
          @bytes = bytes
        when 18 # Human-readable UUID-like representation; inverse of #to_guid
          elements = bytes.split("-")
          raise TypeError, "Expected #{bytes.inspect} to cast to a #{self.class} (malformed UUID-like representation)" if elements.size != 3
          @bytes = [elements.join].pack('H32')
        else
          raise TypeError, "Expected #{bytes.inspect} to cast to a #{self.class} (invalid bytecount)"
        end
      when Integer
        raise TypeError, "Expected #{bytes.inspect} to cast to a #{self.class} (integer out of range)" if bytes < 0 or bytes > 2**64
        @bytes = [bytes >> 32, bytes % 2**32].pack("NN")
      when NilClass, Time
        # Time.stamp is 52 bytes, so we have 12 bytes of entropy left over
        int = ((bytes || Time).stamp << 12) + rand(2**12)
        @bytes = [int >> 32, int % 2**32].pack("NN")
      else
        raise TypeError, "Expected #{bytes.inspect} to cast to a #{self.class} (unknown source class)"
      end
    end

    def to_i
      @to_i ||= begin
        ints = @bytes.unpack("NN")
        (ints[0] << 32) +
        ints[1]
      end
    end

    def to_guid
      "%08x-%04x-%04x" % @bytes.unpack("Nnn")
    end    

    def inspect
      "<Cassandra::Long##{object_id} time: #{
        Time.at((to_i >> 12) / 1_000_000).utc.inspect
      }, usecs: #{
        (to_i >> 12) % 1_000_000
      }, jitter: #{
        to_i % 2**12
      }, guid: #{
        to_guid
      }>"
    end
  end
end
