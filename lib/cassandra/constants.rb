
class TwitterCassandra
  # A helper module you can include in your own class. Makes it easier
  # to work with TwitterCassandra subclasses.
  module Constants
    include TwitterCassandra::Consistency

    Long = TwitterCassandra::Long
    OrderedHash = TwitterCassandra::OrderedHash
  end
end
