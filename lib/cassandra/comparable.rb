
class Cassandra
  # Abstract base class for comparable numeric column name types
  class Comparable
    class TypeError < ::TypeError #:nodoc:
    end
  
    def <=>(other)
      self.to_i <=> other.to_i
    end
    
    def hash
      @bytes.hash
    end
    
    def eql?(other)
      other.is_a?(Comparable) and @bytes == other.to_s
    end    
    
    def ==(other)
      other.respond_to?(:to_i) && self.to_i == other.to_i
    end
    
    def to_s
      @bytes
    end
  end
end
