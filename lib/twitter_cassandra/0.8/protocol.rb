class TwitterCassandra
  # Inner methods for actually doing the Thrift calls
  module Protocol #:nodoc:
    private

    def _remove_counter(key, column_path, consistency_level)
      client.remove_counter(key, column_path, consistency_level)
    end
  end
end
