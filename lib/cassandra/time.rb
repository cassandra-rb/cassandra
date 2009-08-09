
class Time
  def self.stamp
    Time.now.stamp
  end
  
  def stamp
    to_i * 1_000_000 + usec
  end
end
        
