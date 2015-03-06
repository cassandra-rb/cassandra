
class CassandraOld
  # A helper module you can include in your own class. Makes it easier
  # to work with Cassandra subclasses.
  module Constants
    include CassandraOld::Consistency

    Long = CassandraOld::Long
    OrderedHash = CassandraOld::OrderedHash
  end
end
