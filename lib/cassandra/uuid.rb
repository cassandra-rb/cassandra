
class Cassandra

  # UUID format version 1, as specified in RFC 4122, with jitter in place of the mac address and sequence counter.
  class UUID < Comparable    
  
    class InvalidVersion < StandardError; end
    
    GREGORIAN_EPOCH_OFFSET = 0x01B21DD213814000 # Oct 15, 1582
  
    def initialize(bytes = nil)      
      case bytes
      when String
        case bytes.size
        when 16
          @bytes = bytes
        when 36
          elements = bytes.split("-")
          raise TypeError, "Malformed UUID representation" if elements.size != 5
          @bytes = elements.join.to_a.pack('H32')
        else
          raise TypeError, "16 bytes required for byte array, or 36 characters required for UUID representation"
        end
      when Integer
        raise TypeError, "Integer must be between 0 and 2**128" if bytes < 0 or bytes > 2**128
        @bytes = [bytes >> 64, bytes % 2**64].pack("QQ")
      when NilClass        
        time = Time.stamp * 10 + GREGORIAN_EPOCH_OFFSET 
        # See http://github.com/spectra/ruby-uuid/
        @bytes = [time & 0xFFFF_FFFF, time >> 32, ((time >> 48) & 0x0FFF) | 0x1000, rand(2**64)].pack("NnnQ")
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
    
    def <=>(other)
      # Lexical comparison
        to_s <=> (other).to_s
    end    
  
    def version
      time_high = @bytes.unpack("NnnQ")[2]
      version = (time_high & 0xF000).to_s(16)[0].chr.to_i
      if version > 0 and version < 6
        version
      else
        raise InvalidVersion, "Version #{version}"
      end
    end    

    def to_guid
      elements = @bytes.unpack("NnnCCa6")        
      tmp = elements[-1].unpack('C*') 
      elements[-1] = sprintf '%02x%02x%02x%02x%02x%02x', *tmp          
      "%08x-%04x-%04x-%02x%02x-%s" % elements
    end
    
    def seconds_and_usecs
      elements = @bytes.unpack("NnnQ")            
      time = (elements[0] + (elements[1] << 32) + ((elements[2] & 0x0FFF) << 48) - GREGORIAN_EPOCH_OFFSET) / 10
      [time / 1_000_000, time % 1_000_000]
    end
    
    def inspect
      "<Cassandra::UUID##{object_id} time: #{
        Time.at(seconds_and_usecs[0]).inspect
      }, usecs: #{
        seconds_and_usecs[1]
      } jitter: #{
        @bytes.unpack('QQ')[1]
      }, version: #{
        version
      }, guid: #{
        to_guid
      }>"
    end      
  end  
end
