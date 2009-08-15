
=begin rdoc
Create a new Cassandra client instance. Accepts a database name, and optional host and port.

  client = Cassandra.new('twitter', '127.0.0.1', 9160)
  
You can then make calls to the server via the <tt>client</tt> instance.
 
  client.insert(:UserRelationships, "5", {"user_timeline" => {UUID.new => "1"}})
  client.get(:UserRelationships, "5", "user_timeline")
  
For read methods, valid option parameters are:

<tt>:count</tt>:: How many results to return. Defaults to 100.
<tt>:start</tt>:: field name token at which to start iterating, inclusive. Defaults to nil, which means the first field in the collation order.
<tt>:finish</tt>:: field name token at which to stop iterating, inclusive. Defaults to nil, which means no boundary.
<tt>:reversed</tt>:: Swap the direction of the collation order.
<tt>:consistency</tt>:: The consistency level of the request. Defaults to <tt>Cassandra::Consistency::ONE</tt> (one node must respond). Other valid options are <tt>Cassandra::Consistency::ZERO</tt>, <tt>Cassandra::Consistency::QUORUM</tt>, and <tt>Cassandra::Consistency::ALL</tt>.

Note that some read options have no relevance in some contexts.
 
For write methods, valid option parameters are:
 
<tt>:timestamp </tt>:: The transaction timestamp. Defaults to the current time in milliseconds. This is used for conflict resolution by the server; you normally never need to change it.
<tt>:consistency</tt>:: See above.

=end rdoc

class Cassandra
  include Fields
  include Protocol

  class AccessError < StandardError #:nodoc:
  end

  module Consistency
    include CassandraThrift::ConsistencyLevel
  end

  MAX_INT = 2**31 - 1
    
  WRITE_DEFAULTS = {    
    :count => MAX_INT,
    :timestamp => nil,
    :consistency => Consistency::ONE 
  }.freeze

  READ_DEFAULTS = {
    :count => 100, 
    :start => nil, 
    :finish => nil, 
    :reversed => false, 
    :consistency => Consistency::ONE
  }.freeze

  attr_reader :database, :host, :port, :serializer, :transport, :client, :schema

  # Instantiate a new Cassandra and open the connection.
  def initialize(database, host = '127.0.0.1', port = 9160)
    @is_set = {}
    @field_class = {}
    @sub_field_class = {}

    @database = database
    @host = host
    @port = port

    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
    @transport.open
    @client = CassandraThrift::Cassandra::SafeClient.new(
      CassandraThrift::Cassandra::Client.new(Thrift::BinaryProtocol.new(@transport)),
      @transport)

    databases = @client.get_string_list_property("keyspaces")
    unless databases.include?(@database)
      raise AccessError, "Keyspace #{@database.inspect} not found. Available: #{databases.inspect}"
    end

    @schema = @client.describe_keyspace(@database)
  end

  def inspect
    "#<Cassandra:#{object_id}, @database=#{database.inspect}, @schema={#{
      schema.map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @host=#{host.inspect}, @port=#{port}>"
  end

### Write

  # Insert a row for a key. Pass a flat hash for a regular field family, and
  # a nested hash for a super field family. Supports the <tt>:consistency</tt>
  # and <tt>:timestamp</tt> options.
  def insert(row_set, key, hash, options = {})
    row_set, _, _, options = params(row_set, [options], WRITE_DEFAULTS)

    mutation = if is_set(row_set)
      CassandraThrift::BatchMutationSuper.new(
        :key => key, 
        :cfmap => {row_set => 
          hash_to_super_fields(row_set, hash, options[:timestamp] || Time.stamp)})
    else
      CassandraThrift::BatchMutation.new(
        :key => key, 
        :cfmap => {row_set => 
          hash_to_fields(row_set, hash, options[:timestamp] || Time.stamp)})
    end
    
    args = [mutation, options[:consistency]]
    @batch ? @batch << args : _insert(*args)
  end

  ## Delete

  # Remove the element at the row_set:key:[field]:[sub_field]
  # path you request. Supports the <tt>:consistency</tt> and <tt>:timestamp</tt>
  # options.
  def remove(row_set, key, *fields_and_options)
    row_set, field, sub_field, options = params(row_set, fields_and_options, WRITE_DEFAULTS)    
    args = [row_set, key, field, sub_field, options[:consistency], options[:timestamp] || Time.stamp]
    @batch ? @batch << args : _remove(*args)
  end

  # Remove all rows in the field family you request. Supports options 
  # <tt>:consistency</tt> and <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting 
  # for ranged remove support in Cassandra.
  def clear_row_set!(row_set, options = {})
    get_range(row_set).each { |key| remove(row_set, key, options) }
  end

  # Remove all rows in the database. Supports options <tt>:consistency</tt> and 
  # <tt>:timestamp</tt>.
  # FIXME May not currently delete all records without multiple calls. Waiting 
  # for ranged remove support in Cassandra.
  def clear_database!(options = {})
    @schema.keys.each { |row_set| clear_row_set!(row_set, options) }
  end

### Read

  # Count the elements at the row_set:key:[super_field] path you
  # request. Supports options <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, 
  # <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def count_fields(row_set, key, *fields_and_options)
    row_set, super_field, _, options = params(row_set, fields_and_options, READ_DEFAULTS)
    _count_fields(row_set, key, super_field, options[:consistency])
  end

  # Multi-key version of Cassandra#count_fields. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_count_fields(row_set, keys, *options)
    OrderedHash[*keys.map { |key| [key, count_fields(row_set, key, *options)] }._flatten_once]
  end

  # Return a list of single values for the elements at the
  # row_set:key:field[s]:[sub_fields] path you request. Supports the 
  # <tt>:consistency</tt> option.
  def get_fields(row_set, key, *fields_and_options)
    row_set, fields, sub_fields, options = params(row_set, fields_and_options, READ_DEFAULTS)    
    _get_fields(row_set, key, fields, sub_fields, options[:consistency])
  end

  # Multi-key version of Cassandra#get_fields. Supports the <tt>:consistency</tt> 
  # option.
  def multi_get_fields(row_set, keys, *options)
    OrderedHash[*keys.map { |key| [key, get_fields(row_set, key, *options)] }._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the row_set:key:[field]:[sub_field]
  # path you request. Supports options <tt>:count</tt>, <tt>:start</tt>, 
  # <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def get(row_set, key, *fields_and_options)
    row_set, field, sub_field, options = params(row_set, fields_and_options, READ_DEFAULTS)
    _get(row_set, key, field, sub_field, options[:count], options[:start], options[:finish], options[:reversed], options[:consistency])
  rescue CassandraThrift::NotFoundException
    is_set(row_set) && !sub_field ? OrderedHash.new : nil
  end

  # Multi-key version of Cassandra#get. Supports options <tt>:count</tt>, 
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_get(row_set, keys, *options)
    OrderedHash[*keys.map { |key| [key, get(row_set, key, *options)] }._flatten_once]
  end

  # Return true if the row_set:key:[field]:[sub_field] path you
  # request exists. Supports the <tt>:consistency</tt> option.
  def exists?(row_set, key, *fields_and_options)
    row_set, field, sub_field, options = params(row_set, fields_and_options, READ_DEFAULTS)    
    _get(row_set, key, field, sub_field, 1, nil, nil, nil, options[:consistency])
    true
  rescue CassandraThrift::NotFoundException
  end

  # Return a list of keys in the row_set you request. Requires the
  # table to be partitioned with OrderPreservingHash. Supports the 
  # <tt>:count</tt>, <tt>:start</tt>, <tt>:finish</tt>, and <tt>:consistency</tt> 
  # options.
  def get_range(row_set, options = {})
    row_set, _, _, options = params(row_set, [options], READ_DEFAULTS)
    _get_range(row_set, options[:start], options[:finish], options[:count], options[:consistency])
  end

  # Count all rows in the row_set you request. Requires the table
  # to be partitioned with OrderPreservingHash. Supports the <tt>:start</tt>, 
  # <tt>:finish</tt>, and <tt>:consistency</tt> options.
  # FIXME will count only MAX_INT records
  def count_range(row_set, options = {})
    get_range(row_set, options.merge(:count => MAX_INT)).size
  end

  # Open a batch operation and yield. Inserts and deletes will be queued until
  # the block closes, and then sent atomically to the server.
  # FIXME Make deletes truly atomic.
  def batch
    @batch = []
    yield
    compact_mutations
    dispatch_mutations
    @batch = nil
  end
  
  private
  
  # Extract and validate options.
  def params(row_set, args, options)
    if args.last.is_a?(Hash)      
      if (extras = args.last.keys - options.keys).any?
        this = "#{self.class}##{caller[0].split('`').last[0..-2]}"
        raise ArgumentError, "Invalid options #{extras.inspect[1..-2]} for #{this}"
      end
      options = options.merge(args.pop)
    end    

    row_set, field, sub_field = row_set.to_s, args[0], args[1]
    assert_field_classes(row_set, field, sub_field)    
    [row_set, map_to_s(field), map_to_s(sub_field), options]
  end
  
  # Convert stuff to strings.
  def map_to_s(el)
    case el
    when NilClass # nil
    when Array then el.map { |i| map_to_s(i) }
    when Comparable, String, Symbol then el.to_s
    else
      raise Comparable::TypeError, "Can't map #{el.inspect}"
    end
  end
  
  # Roll up queued mutations as much as possible, to improve atomicity.
  def compact_mutations
    compact_batch = []
    mutations = {}

    @batch << nil # Close it
    @batch.each do |m|
      case m
      when Array, nil
        # Flush compacted mutations
        compact_batch.concat(mutations.values.map {|x| x.values}.flatten)
        mutations = {}
        # Insert delete operation
        compact_batch << m
      else # BatchMutation, BatchMutationSuper
        # Do a nested hash merge
        if mutation_class = mutations[m.class]
          if mutation = mutation_class[m.key]
            if fields = mutation.cfmap[m.cfmap.keys.first]
              fields.concat(m.cfmap.values.first)
            else
              mutation.cfmap.merge!(m.cfmap)
            end
          else
            mutation_class[m.key] = m
          end
        else
          mutations[m.class] = {m.key => m}
        end
      end
    end

    @batch = compact_batch
  end

  # Send all the queued mutations to the server.
  def dispatch_mutations
    @batch.compact!
    @batch.each do |args|
      case args.first
      when CassandraThrift::BatchMutationSuper, CassandraThrift::BatchMutation
        _insert(*args)
      else
        _remove(*args)
      end
    end
  end  
end
