class CassandraClient
  attr_reader :client, :transport, :tables, :host, :port, :block_for, :serialization

  class AccessError < StandardError; end

  # Instantiate a new CassandraClient and open the connection.
  def initialize(host = '127.0.0.1', port = 9160, block_for = 1, serialization = CassandraClient::Serialization::JSON)
    @host = host
    @port = port
    @serialization = serialization
    @block_for = block_for
    
    @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@host, @port))
    @transport.open

    @client = SafeClient.new(
      Cassandra::Client.new(Thrift::BinaryProtocol.new(@transport)), 
      @transport)

    @tables = @client.getStringListProperty("tables").map do |table_name|
      ::CassandraClient::Table.new(table_name, self)
    end
  end

  def inspect(full = true)
    string = "#<CassandraClient:#{object_id}, @host=#{host.inspect}, @port=#{@port.inspect}"
    string += ", @block_for=#{block_for.inspect}, @tables=[#{tables.map {|t| t.inspect(false) }.join(', ')}]" if full
    string + ">"
  end

  # Return the CassandraClient::Table instance for the table_name you
  # request. You can get an array of all available tables with the #tables
  # method.
  def table(table_name)
    table = @tables.detect {|table| table.name == table_name }
    raise AccessError, "No such table #{table_name.inspect}" unless table
    table
  end

  # Remove all rows in all column families in all tables.
  def remove_all
    tables.each do |table|
      table.schema.keys.each do |column_family|
        table.remove_all(column_family)
      end
    end
  end
  
  class SafeClient  
    def initialize(client, transport)
      @client = client 
      @transport = transport
    end
    
    def method_missing(*args)
      @client.send(*args)
    rescue IOError
      @transport.open
      raise if defined?(once)
      once = true
      retry
    end
  end
  
end
