
if [ ! -e cassandra ]; then
  rm -rf data
  tar xjf vendor/cassandra.tar.bz2
fi

cd cassandra && ant && cd ..

env CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh cassandra/bin/cassandra -f
