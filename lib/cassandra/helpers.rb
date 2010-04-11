class Cassandra
  module Helpers
    def extract_and_validate_params(column_family, keys, args, options)
      options = options.dup
      column_family = column_family.to_s
      # Keys
      [keys].flatten.each do |key|
        raise ArgumentError, "Key #{key.inspect} must be a String for #{caller[2].inspect}." unless key.is_a?(String)
      end

      # Options
      if args.last.is_a?(Hash)
        extras = args.last.keys - options.keys
        raise ArgumentError, "Invalid options #{extras.inspect[1..-2]} for #{caller[1]}" if extras.any?
        options.merge!(args.pop)      
      end

      # Ranges
      column, sub_column = args[0], args[1]
      klass, sub_klass = column_name_class(column_family), sub_column_name_class(column_family)
      range_class = column ? sub_klass : klass

      [:start, :finish].each do |opt|
        options[opt] = options[opt] ? range_class.new(options[opt]).to_s : ''
      end

      [column_family, s_map(column, klass), s_map(sub_column, sub_klass), options]
    end

    # Convert stuff to strings.
    def s_map(el, klass)
      case el
      when Array then el.map { |i| s_map(i, klass) }
      when NilClass then nil
      else
        klass.new(el).to_s
      end
    end
  end
end
