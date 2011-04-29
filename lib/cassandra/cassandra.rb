
=begin rdoc
Create a new Cassandra client instance. Accepts a keyspace name, and optional host and port.

  client = Cassandra.new('twitter', '127.0.0.1:9160')
  
If the server requires authentication, you must authenticate before make calls

  client.login!('username','password')

You can then make calls to the server via the <tt>client</tt> instance.

  client.insert(:UserRelationships, "5", {"user_timeline" => {SimpleUUID::UUID.new => "1"}})
  client.get(:UserRelationships, "5", "user_timeline")

For read methods, valid option parameters are:

<tt>:count</tt>:: How many results to return. Defaults to 100.
<tt>:start</tt>:: Column name token at which to start iterating, inclusive. Defaults to nil, which means the first column in the collation order.
<tt>:finish</tt>:: Column name token at which to stop iterating, inclusive. Defaults to nil, which means no boundary.
<tt>:reversed</tt>:: Swap the direction of the collation order.
<tt>:consistency</tt>:: The consistency level of the request. Defaults to <tt>Cassandra::Consistency::ONE</tt> (one node must respond). Other valid options are <tt>Cassandra::Consistency::ZERO</tt>, <tt>Cassandra::Consistency::QUORUM</tt>, and <tt>Cassandra::Consistency::ALL</tt>.

Note that some read options have no relevance in some contexts.

For write methods, valid option parameters are:

<tt>:timestamp </tt>:: The transaction timestamp. Defaults to the current time in milliseconds. This is used for conflict resolution by the server; you normally never need to change it.
<tt>:consistency</tt>:: See above.

For the initial client instantiation, you may also pass in <tt>:thrift_client<tt> with a ThriftClient subclass attached. On connection, that class will be used instead of the default ThriftClient class, allowing you to add additional behavior to the connection (e.g. query logging).

=end rdoc

class Cassandra
  include Columns
  include Protocol
  include Helpers

  class AccessError < StandardError #:nodoc:
  end

  module Consistency
    include CassandraThrift::ConsistencyLevel
  end

  WRITE_DEFAULTS = {
    :count => 1000,
    :timestamp => nil,
    :consistency => Consistency::ONE,
    :ttl => nil
  }.freeze

  READ_DEFAULTS = {
    :count => 100,
    :start => nil,
    :finish => nil,
    :start_key => nil,
    :finish_key => nil,
    :reversed => false,
    :consistency => Consistency::ONE
  }.freeze
  
  THRIFT_DEFAULTS = {
    :transport_wrapper => Thrift::BufferedTransport,
    :thrift_client_class => ThriftClient
  }.freeze

  attr_reader :keyspace, :servers, :schema, :thrift_client_options, :thrift_client_class, :auth_request

  # Create a new Cassandra instance and open the connection.
  def initialize(keyspace, servers = "127.0.0.1:9160", thrift_client_options = {})
    @is_super = {}
    @column_name_class = {}
    @sub_column_name_class = {}
    @auto_discover_nodes = true
    thrift_client_options[:transport_wrapper] ||= Cassandra.DEFAULT_TRANSPORT_WRAPPER
    @thrift_client_options = THRIFT_DEFAULTS.merge(thrift_client_options)
    @thrift_client_class = @thrift_client_options[:thrift_client_class]
    @keyspace = keyspace
    @servers = Array(servers)
  end

  def disable_node_auto_discovery!
    @auto_discover_nodes = false
  end

  def disconnect!
    if @client
      @client.disconnect!
      @client = nil
    end
  end

  def keyspaces
    @keyspaces ||= client.describe_keyspaces()
  end
  
  def login!(username, password)
    @auth_request = CassandraThrift::AuthenticationRequest.new
    @auth_request.credentials = {'username' => username, 'password' => password}
    client.login(@keyspace, @auth_request)
  end
  
  def inspect
    "#<Cassandra:#{object_id}, @keyspace=#{keyspace.inspect}, @schema={#{
      schema(false).map {|name, hash| ":#{name} => #{hash['type'].inspect}"}.join(', ')
    }}, @servers=#{servers.inspect}>"
  end

### Write

  # Insert a row for a key. Pass a flat hash for a regular column family, and
  # a nested hash for a super column family. Supports the <tt>:consistency</tt>,
  # <tt>:timestamp</tt> and <tt>:ttl</tt> options.
  def insert(column_family, key, hash, options = {})
    column_family, _, _, options = extract_and_validate_params(column_family, key, [options], WRITE_DEFAULTS)

    timestamp = options[:timestamp] || Time.stamp
    mutation_map = if is_super(column_family)
      {
        key => {
          column_family => hash.collect{|k,v| _super_insert_mutation(column_family, k, v, timestamp, options[:ttl]) }
        }
      }
    else
      {
        key => {
          column_family => hash.collect{|k,v| _standard_insert_mutation(column_family, k, v, timestamp, options[:ttl])}
        }
      }
    end

    @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
  end


  ## Delete

  # _mutate the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports the <tt>:consistency</tt> and <tt>:timestamp</tt>
  # options.
  # TODO: we could change this function or add another that support multi-column removal (by list or predicate)
  def remove(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = extract_and_validate_params(column_family, key, columns_and_options, WRITE_DEFAULTS)

    if @batch
      mutation_map = 
        {
          key => {
            column_family => [ _delete_mutation(column_family, column, sub_column, options[:timestamp]|| Time.stamp) ]
          }
        }
      @batch << [mutation_map, options[:consistency]]
    else 
      # Let's continue using the 'remove' thrift method...not sure about the implications/performance of using the mutate instead
      # Otherwise we coul get use the mutation_map above, and do _mutate(mutation_map, options[:consistency])
      args = {:column_family => column_family}
      columns = is_super(column_family) ? {:super_column => column, :column => sub_column} : {:column => column}
      column_path = CassandraThrift::ColumnPath.new(args.merge(columns))
      _remove(key, column_path, options[:timestamp] || Time.stamp, options[:consistency])
    end
  end

### Read

  # Count the elements at the column_family:key:[super_column] path you
  # request. Supports the <tt>:consistency</tt> option.
  def count_columns(column_family, key, *columns_and_options)
    column_family, super_column, _, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _count_columns(column_family, key, super_column, options[:consistency])
  end

  # Multi-key version of Cassandra#count_columns. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  # FIXME Not real multi; needs server support
  def multi_count_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, count_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a list of single values for the elements at the
  # column_family:key:column[s]:[sub_columns] path you request. Supports the
  # <tt>:consistency</tt> option.
  def get_columns(column_family, key, *columns_and_options)
    column_family, columns, sub_columns, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)      
    _get_columns(column_family, key, columns, sub_columns, options[:consistency])
  end

  # Multi-key version of Cassandra#get_columns. Supports the <tt>:consistency</tt>
  # option.
  # FIXME Not real multi; needs to use a Column predicate
  def multi_get_columns(column_family, keys, *options)
    OrderedHash[*keys.map { |key| [key, get_columns(column_family, key, *options)] }._flatten_once]
  end

  # Return a hash (actually, a Cassandra::OrderedHash) or a single value
  # representing the element at the column_family:key:[column]:[sub_column]
  # path you request. Supports options <tt>:count</tt>, <tt>:start</tt>,
  # <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def get(column_family, key, *columns_and_options)
    multi_get(column_family, [key], *columns_and_options)[key]
  end

  # Multi-key version of Cassandra#get. Supports options <tt>:count</tt>,
  # <tt>:start</tt>, <tt>:finish</tt>, <tt>:reversed</tt>, and <tt>:consistency</tt>.
  def multi_get(column_family, keys, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, keys, columns_and_options, READ_DEFAULTS)

    hash = _multiget(column_family, keys, column, sub_column, options[:count], options[:start], options[:finish], options[:reversed], options[:consistency])
    # Restore order
    ordered_hash = OrderedHash.new
    keys.each { |key| ordered_hash[key] = hash[key] || (OrderedHash.new if is_super(column_family) and !sub_column) }
    ordered_hash
  end

  # Return true if the column_family:key:[column]:[sub_column] path you
  # request exists. Supports the <tt>:consistency</tt> option.
  def exists?(column_family, key, *columns_and_options)
    column_family, column, sub_column, options = 
      extract_and_validate_params(column_family, key, columns_and_options, READ_DEFAULTS)
    if column
      _multiget(column_family, [key], column, sub_column, 1, nil, nil, nil, options[:consistency])[key]
    else
      _multiget(column_family, [key], nil, nil, 1, '', '', false, options[:consistency])[key]
    end
  end

  # Return a list of keys in the column_family you request. Only works well if
  # the table is partitioned with OrderPreservingPartitioner. Supports the
  # <tt>:count</tt>, <tt>:start_key</tt>, <tt>:finish_key</tt>, and
  # <tt>:consistency</tt> options.
  def get_range(column_family, options = {})
    column_family, _, _, options = 
      extract_and_validate_params(column_family, "", [options], READ_DEFAULTS)
    _get_range(column_family, options[:start_key].to_s, options[:finish_key].to_s, options[:count], options[:consistency])
  end

  # Count all rows in the column_family you request. Requires the table
  # to be partitioned with OrderPreservingHash. Supports the <tt>:start</tt>,
  # <tt>:finish</tt>, and <tt>:consistency</tt> options.
  def count_range(column_family, options = {})
    get_range(column_family, options).select{|r| r.columns.length > 0}.compact.length
  end

  # Open a batch operation and yield self. Inserts and deletes will be queued
  # until the block closes, and then sent atomically to the server.  Supports
  # the <tt>:consistency</tt> option, which overrides the consistency set in
  # the individual commands.
  def batch(options = {})
    _, _, _, options = 
      extract_and_validate_params(schema.keys.first, "", [options], WRITE_DEFAULTS)

    @batch = []
    yield(self)
    compacted_map,seen_clevels = compact_mutations!
    clevel = if options[:consistency] != nil # Override any clevel from individual mutations if 
               options[:consistency]
             elsif seen_clevels.length > 1 # Cannot choose which CLevel to use if there are several ones
               raise "Multiple consistency levels used in the batch, and no override...cannot pick one" 
             else # if no consistency override has been provided but all the clevels in the batch are the same: use that one
               seen_clevels.first
             end
        
    _mutate(compacted_map,clevel)
  ensure
    @batch = nil
  end
  
  protected

  def calling_method
    "#{self.class}##{caller[0].split('`').last[0..-3]}"
  end

  # Roll up queued mutations, to improve atomicity (and performance).
  def compact_mutations!
    used_clevels = {} # hash that lists the consistency levels seen in the batch array. key is the clevel, value is true
    by_key = Hash.new{|h,k | h[k] = {}}
    # @batch is an array of mutation_ops.
    # A mutation op is a 2-item array containing [mutationmap, consistency_number]
    # a mutation map is a hash, by key (string) that has a hash by CF name, containing a list of column_mutations)
    @batch.each do |mutation_op|
      # A single mutation op looks like:
      # For an insert/update
      #[ { key1 => 
      #            { CF1 => [several of CassThrift:Mutation(colname,value,TS,ttl)]
      #              CF2 => [several mutations]
      #            },
      #    key2 => {...} # Not sure if they can come batched like this...so there might only be a single key (and CF)
      #      }, # [0]
      #  consistency # [1] 
      #]
      mmap = mutation_op[0] # :remove OR a hash like {"key"=> {"CF"=>[mutationclass1,...] } }
      used_clevels[mutation_op[1]] = true #save the clevel required for this operation

      mmap.keys.each do |k|
        mmap[k].keys.each do |cf| # For each CF in that key
          by_key[k][cf] ||= []
          by_key[k][cf].concat(mmap[k][cf]) # Append the list of mutations for that key and CF
        end
      end
    end
    # Returns the batch mutations map, and an array with the consistency levels 'seen' in the batch
    [by_key, used_clevels.keys]
  end

  def new_client
    thrift_client_class.new(CassandraThrift::Cassandra::Client, @servers, @thrift_client_options)
  end
  
end
