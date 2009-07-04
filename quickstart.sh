
if [ ! -e vendor/cassandra-r791040 ]; then
  cd vendor
  tar xjvf cassandra-r791040.tar.bz2
  cd ..
fi

export CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh
vendor/cassandra-r791040/bin/cassandra -f
