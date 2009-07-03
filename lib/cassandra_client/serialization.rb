
class CassandraClient
  module Serialization
    module String
      def dump(object)
        object.to_s
      end
      
      def load(object)
        object
      end
    end
  
    module Marshal
      def dump(object)
        ::Marshal.dump(object)
      end

      def load(object)
        ::Marshal.load(object)
      end
    end

    module JSON
      def dump(object)
        ::JSON.dump(object)
      end

      begin
        require 'yajl/json_gem'
        def load(object)
          ::JSON.load(object)
        end
      rescue LoadError      
        require 'json/ext'        
        def load(object)
          ::JSON.load("[#{object}]").first # :-(
        end
      end
    end
      
    module CompressedJSON
      include JSON
      def dump(object)
        Zlib::Deflate.deflate(super(object))
      end

      def load(object)        
        super(Zlib::Inflate.inflate(object))
      end
    end
    
    # module Avro
    #  # Someday!
    # end
  end
end
