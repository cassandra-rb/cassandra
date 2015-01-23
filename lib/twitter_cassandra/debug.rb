
require 'pp'

class CassandraThrift::Cassandra::Client
  def send_message(*args)
    pp args
    super
  end
end
