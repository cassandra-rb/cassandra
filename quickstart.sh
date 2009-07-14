
if [ ! -e cassandra ]; then
  cd vendor
  tar xjvf cassandra-r789419.tar.bz2
  mv cassandra-r789419 ../cassandra
  cd ..
fi

env CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh cassandra/bin/cassandra -f
