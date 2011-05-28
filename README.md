# cassandra 
A Ruby client for the Cassandra distributed database.

* [Getting Started](#getting-started)
* [License](#license)
* [Cassandra Version](#cassandra-version)
* [Read/Write API Method Reference](#read-write-api-method-reference)

## Getting Started

Here is a quick sample of the general use (more details in Read/Write
API below):

    require 'cassandra'
    client = Cassandra.new('Twitter', '127.0.0.1:9160')
    client.insert(:Users, "5", {'screen_name' => "buttonscat"})

## License

Copyright 2009, 2010 Twitter, Inc. See included LICENSE file. Portions copyright 2004-2009 David Heinemeier Hansson, and used with permission.

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

## Read/Write API Method Reference

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

## Reporting Problems

The Github issue tracker is [here](http://github.com/fauna/cassandra/issues). If you have problems with this library or Cassandra itself, please use the [cassandra-user mailing list](http://mail-archives.apache.org/mod_mbox/incubator-cassandra-user/).
