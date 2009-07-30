
class CassandraClient
  # A temporally-ordered Long class for use in Cassandra super columns.
  class Long < Comparable
    ENTROPY = 2**12
    
    def initialize(bytes = nil)      
      if bytes
        raise TypeError, "8 bytes required" if bytes.size != 8
        @bytes = bytes
      else        
        # Time.stamp is 52 bytes, so we have 12 bytes of entropy left over
        @bytes = [Time.stamp * ENTROPY + rand(ENTROPY)].pack("Q")
      end
    end

    def to_i
      @bytes.unpack("Q")
    end    
    
    def inspect
      ints = @bytes.unpack("Q")
      "<CassandraClient::Long##{object_id} time: #{
          Time.at((ints[0] / ENTROPY) / 1_000_000).inspect
        }, usecs: #{
          (ints[0] / ENTROPY) % 1_000_000
        }, jitter: #{
          ints[0] % ENTROPY
        }>"
    end      
  end  
end
