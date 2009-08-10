
class Cassandra
  module Constants
    include Cassandra::Consistency
        
    UUID = Cassandra::UUID
    Long = Cassandra::Long
    OrderedHash = Cassandra::OrderedHash
  end
end
