# cassandra 
A Ruby client for the Cassandra distributed database.

* [Getting Started](#getting-started)
* [Cassandra Version](#cassandra-version)
* [API Method Reference](#api-method-reference)

## Getting Started

This is where we'll specify a simple list of operations.

## Cassandra Version

The Cassandra project is under very active development, and as such
there are a few different versions that you may need to use this gem
with.  We have set up an easy sure fire mechanism for selecting the
specific version that you are connecting to while requiring the gem.

#### Require Method
The default version is the currently stable release of cassandra.  (0.7
at this time, but 0.8 is looming in the near future.)

To use the default version simply use a normal require:
    require 'cassandra'

To use a specific version (0.7 in this example) you would use a 
slightly differently formatted require:
    require 'cassandra/0.7'

#### Environment Variable Method
These mechanisms work well when you are using the cassandra gem in your
own projects or irb, but if you would rather not hard code your app to a
specific version you can always specify an environment variable with the
version you are using:
    export CASSANDRA_VERSION=0.7

Then you would use the default require as listed above:
    require 'cassandra'

## API Method Reference

### disable\_node\_auto\_discovery!

This method will prevent us from trying to auto-discover all the
server addresses, and only use the list of servers provided on
initialization.

This is primarily helpful when the cassandra cluster is communicating
internally on a different ip address than what you are using to connect.
A prime example of this would be when using EC2 to host a cluster.
Typically, the cluster would be communicating over the local ip
addresses issued by Amazon, but any clients connecting from outside EC2
would need to use the public ip.

### disconnect!

This method will disconnect the current client.

### keyspaces

Returns an array of the available keyspaces.

### login!

* username
* password

Issues a login attempt using the username and password specified.

### default\_write\_consistency=

The initial default consistency is set to ONE, but you can use
this method to override the normal default with your specified value.
Use this if you do not want to specify a write consistency for each
insert statement.

### default\_read\_consistency=

The initial default consistency is set to ONE, but you can use
this method to override the normal default with your specified value.
Use this if you do not want to specify a read consistency for each query.

### insert

* column\_family - The column\_family that you are inserting into.
* key - The row key to insert.
* hash - The columns or super columns to insert.
* options - Valid options are:
  * :timestamp - Uses the current time if none specified.
  * :consistency - Uses the default write consistency if none specified.
  * :ttl - If specified this is the number of seconds after the insert that this value will be available.

This is the main method used to insert rows into cassandra. If the
column\_family that you are inserting into is a SuperColumnFamily then
the hash passed in should be a nested hash, otherwise it should be a
flat hash.

This method can also be called while in batch mode. If in batch mode
then we queue up the mutations (an insert in this case) and pass them to
cassandra in a single batch at the end of the block.

### remove

* column\_family - The column\_family that you are inserting into.
* key - The row key to insert.
* columns\_and\_options - The columns or super columns to insert.
* options - Valid options are:
  * :timestamp - Uses the current time if none specified.
  * :consistency - Uses the default write consistency if none specified.

This method is used to delete (actually marking them as deleted with a
tombstone) columns or super columns.

### count\_columns

* column\_family - The column\_family that you are inserting into.
* key - The row key to insert.
* columns\_and\_options - The columns or super columns to insert.
* options - Valid options are:
  * :consistency - Uses the default read consistency if none specified.
