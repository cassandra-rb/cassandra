
class << Time
  def now_in_useconds
    time = Time.now
    time.to_i * 1_000_000 + time.usec
  end  
  alias :timestamp :now_in_useconds
end
        
