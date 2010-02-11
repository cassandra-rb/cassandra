module Connection
  class HTTP < Base

  private

    def force_connection(server)
      uri = parse_server(server)
      @transport = Thrift::HTTPClientTransport.new(server)
      Net::HTTP.get(uri)
      # TODO http.use_ssl = @url.scheme == "https"
    end
  
    def parse_server(server)
      uri = URI.parse(server)
      raise ArgumentError, 'Servers must start with http' unless uri.scheme =~ /^http/
      uri
    end
  end
end