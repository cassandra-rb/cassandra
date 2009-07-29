
# A temporally-ordered UUID for use in Cassandra super columns.
class UUID
  MAX_UINT = 2**32
  
  def initialize(bytes = nil)      
    if bytes
      @bytes = bytes
    else
      time = Time.now
      # FIXME Should put the timestamp into a 64-bit uint, rather than two
      # 32-bit uints, which suffer from the 2038 problem.
      @bytes = [time.to_i, time.usec, Process.pid, rand(MAX_UINT)].pack("I*")
    end
  end
  
  def to_i
    value = 0
    @bytes.unpack("I*").each_with_index do |int, position|
      value += int * (MAX_UINT ** position)
    end
    value
  end
  
  def <=>(other)
    self.to_i <=> other.to_i
  end
  
  def to_s
    @bytes
  end
  
  def inspect
    ints = @bytes.unpack("I*")
    "<CassandraClient::UUID##{object_id} time: #{Time.at(ints[0]).inspect}, usecs: #{ints[1]}, pid: #{ints[2]}, jitter: #{ints[3]}>"
  end      
end

class String
  def to_uuid
    ::UUID.new(self)
  end
end
