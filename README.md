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

The default version is the currently stable release of cassandra.  (0.7
at this time, but 0.8 is looming in the near future.)

To use the default version simply use a normal require:
    require 'cassandra'

To use a specific version (0.7 in this example) you would use a 
slightly differently formatted require:
    require 'cassandra/0.7'

These mechanisms work well when you are using the cassandra gem in your
own projects or irb, but if you would rather not hard code your app to a
specific version you can always specify an environment variable with the
version you are using:
    export CASSANDRA_VERSION=0.7

Then you would use the default require as listed above:
    require 'cassandra'

## API Method Reference
