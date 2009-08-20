
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
        @bytes = [bytes >> 32, bytes % 2**32].pack("NN")
      when NilClass, Time
        # Time.stamp is 52 bytes, so we have 12 bytes of entropy left over
        int = ((bytes || Time).stamp << 12) + rand(2**12)
        @bytes = [int >> 32, int % 2**32].pack("NN")
      else
        raise TypeError, "Can't convert from #{bytes.class}"
      end
    end

    def to_i
      @to_i ||= begin
        ints = @bytes.unpack("NN")
        (ints[0] << 32) + 
        ints[1]
      end
    end    
    
    def inspect
      "<Cassandra::Long##{object_id} time: #{
        Time.at((to_i >> 12) / 1_000_000).inspect
      }, usecs: #{
        (to_i >> 12) % 1_000_000
      }, jitter: #{
        to_i % 2**12
      }>"
    end      
  end  
end
