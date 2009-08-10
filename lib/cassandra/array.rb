
class Array
  def _flatten_once
    result = []
    each { |el| result.concat(Array(el)) }
    result
  end  
end
