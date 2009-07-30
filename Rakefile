require 'echoe'

Echoe.new("cassandra") do |p|
  p.author = "Evan Weaver"
  p.project = "fauna"
  p.summary = "A Ruby client for the Cassandra distributed database."
  p.rubygems_version = ">= 0.8"
  p.dependencies = ['json']
  p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
  p.rdoc_pattern = /^(lib|bin|tasks|ext)|_types.rb|_constants.rb|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
  p.url = "http://blog.evanweaver.com/files/doc/fauna/cassandra/"
  p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
end
