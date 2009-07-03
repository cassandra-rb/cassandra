class CassandraClient
  attr_reader :client, :transport, :tables, :host, :port, :block_for
  
  class AccessError < StandardError; end

  # Instantiate a new CassandraClient and open the connection.
  def initialize(host = '127.0.0.1', port = 9160, block_for = 1)
    @host, @port = host, port
    socket = Thrift::Socket.new(@host, @port)
    @transport = Thrift::BufferedTransport.new(socket)
    protocol = Thrift::BinaryProtocol.new(@transport)    
    @client = Cassandra::Client.new(protocol)    
    @block_for = block_for
    
    @transport.open    
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
end
 