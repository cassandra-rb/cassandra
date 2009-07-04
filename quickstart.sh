
if [ ! -e cassandra-r789419 ]; then
  cd vendor
  tar xjvf cassandra-r789419.tar.bz2
  mv cassandra-r789419 ..
  cd ..
fi

export CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh
cassandra-r789419/bin/cassandra -f
