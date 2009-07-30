
class CassandraClient
  # A temporally-ordered UUID for use in Cassandra super columns.
  class UUID
    MAX_UINT = 2**32
    
    def initialize(bytes = nil)      
      if bytes
        @bytes = bytes
      else
        @bytes = [Time.stamp, Process.pid, rand(MAX_UINT)].pack("QII")
      end
    end
    
    def to_i
      value = 0
      @bytes.unpack("QII").each_with_index do |int, position|
        value += int * (MAX_UINT ** position)
      end
      value
    end
    
    def <=>(other)
      self.to_i <=> other.to_i
    end
    
    def eql?(other)
      @bytes == other.to_s
    end    
    alias :"==" :"eql?"
    
    def to_s
      @bytes
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
