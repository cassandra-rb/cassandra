
class Cassandra
  # A temporally-ordered Long class for use in Cassandra column names
  class Long < Comparable
  
    def initialize(bytes = nil)      
      case bytes
      when String
        raise TypeError, "8 bytes required" if bytes.size != 8
        @bytes = bytes
      when Integer
        raise TypeError, "Integer must be between 0 and 2**64" if bytes < 0 or bytes > 2**64
        @bytes = [bytes].pack("Q")
      when NilClass
        # Time.stamp is 52 bytes, so we have 12 bytes of entropy left over
        @bytes = [(Time.stamp << 12) + rand(2**12)].pack("Q")
      else
        raise TypeError, "Can't convert from #{bytes.class}"
      end
    end

    def to_i
      @to_i ||= @bytes.unpack("Q")[0]
    end    
    
    def inspect
      ints = @bytes.unpack("Q")
      "<Cassandra::Long##{object_id} time: #{
          Time.at((ints[0] >> 12) / 1_000_000).inspect
        }, usecs: #{
          (ints[0] >> 12) % 1_000_000
        }, jitter: #{
          ints[0] % ENTROPY_MAX
        }>"
    end      
  end  
end
