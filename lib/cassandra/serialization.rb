
class Cassandra
  module Serialization
    module String
      def self.dump(object);
        object.to_s
      end

      def self.load(object)
        object
      end
    end

    module Marshal
      def self.dump(object)
        ::Marshal.dump(object)
      end

      def self.load(object)
        ::Marshal.load(object)
      end
    end

    module JSON
      def self.dump(object)
        ::JSON.dump(object)
      end

      begin
        require 'yajl/json_gem'
        def self.load(object)
          ::JSON.load(object)
        end
      rescue LoadError
        require 'json/ext'
        def self.load(object)
          ::JSON.load("[#{object}]").first # :-(
        end
      end
    end
    
    module CompressedJSON
      def self.dump(object)
        Zlib::Deflate.deflate(JSON.dump(object))
      end

      def self.load(object)
        JSON.load(Zlib::Inflate.inflate(object))
      end    
    end

    # module Avro
    #  # Someday!
    # end
  end
end

