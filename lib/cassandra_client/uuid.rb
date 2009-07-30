
class CassandraClient
  # A temporally-ordered UUID for use in Cassandra super columns.
  class UUID < Comparable
    MAX_UINT = 2**32
    
    def initialize(bytes = nil)      
      if bytes
        raise TypeError, "16 bytes required" if bytes.size != 16
        @bytes = bytes
      else
        @bytes = [Time.stamp, Process.pid, rand(MAX_UINT)].pack("QII")
      end
    end
    
    def to_i
      @to_i ||= begin
        ints = @bytes.unpack("QQ")
        # FIXME I doubt this is right
        ints[0] * 2**64 + ints[1]        
      end
    end
    
    def inspect
      ints = @bytes.unpack("QII")
      "<CassandraClient::UUID##{object_id} time: #{
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
