
if [ ! -e cassandra ]; then
  rm -rf data
  cd vendor
  tar xjf cassandra.tar.bz2
  mv cassandra ..
  cd ../cassandra
  ant
  cd ..
fi

env CASSANDRA_INCLUDE=`pwd`/conf/cassandra.in.sh cassandra/bin/cassandra -f
