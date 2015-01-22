class TwitterCassandra
  class Batch
    include Enumerable

    def initialize(cassandra, options)
      @queue_size = options.delete(:queue_size) || 0
      @cassandra = cassandra
      @options = options
      @batch_queue = []
    end

    ##
    # Append mutation to the batch queue
    # Flush the batch queue if full
    #
    def <<(mutation)
      @batch_queue << mutation
      if @queue_size > 0 and @batch_queue.length >= @queue_size
        begin
          @cassandra.flush_batch(@options)
        ensure
          @batch_queue = []
        end
      end
    end

    ##
    # Implement each method (required by Enumerable)
    #
    def each(&block)
      @batch_queue.each(&block)
    end

    ##
    # Queue size
    #
    def length
      @batch_queue.length
    end
  end
end
