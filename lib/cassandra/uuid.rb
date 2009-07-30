
class Cassandra
  # A temporally-ordered UUID class for use in Cassandra column names
  class UUID < Comparable    
  
    def initialize(bytes = nil)      
      case bytes
      when String
        raise TypeError, "16 bytes required" if bytes.size != 16
        @bytes = bytes
      when Integer
        raise TypeError, "Integer must be between 0 and 2**128" if bytes < 0 or bytes > 2**128
        @bytes = [bytes >> 64, bytes % 2**64].pack("QQ")
      when NilClass
        @bytes = [Time.stamp, Process.pid, rand(2**32)].pack("QII")
      else
        raise TypeError, "Can't convert from #{bytes.class}"
      end
    end
    
    def to_i
      @to_i ||= begin
        ints = @bytes.unpack("QQ")
        (ints[0] << 64) + ints[1]        
      end
    end
    
    def inspect
      ints = @bytes.unpack("QII")
      "<Cassandra::UUID##{object_id} time: #{
          Time.at(ints[0] / 1_000_000).inspect
        }, usecs: #{
          ints[0] % 1_000_000
        }, pid: #{
          ints[1]
        }, jitter: #{
          ints[2]
        }>"
    end      
  end  
end
