
if [ ! -e "cassandra-r789419" ]; then
  tar xjvf cassandra-r789419.tar.bz2
fi

export CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh
cassandra-r789419/bin/cassandra -f
