
class CassandraClient
  # Abstract base class for comparable numeric column name types
  class Comparable
    class TypeError < ::TypeError
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
  end
end
