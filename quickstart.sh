
if [ ! -e vendor/cassandra-r789419 ]; then
  cd vendor
  tar xjvf cassandra-r789419.tar.bz2
  cd ..
fi

export CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh
vendor/cassandra-r789419/bin/cassandra -f
