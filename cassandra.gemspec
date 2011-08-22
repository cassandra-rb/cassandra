# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{cassandra}
  s.version = "0.12.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0.8") if s.respond_to? :required_rubygems_version=
  s.authors = [%q{Evan Weaver, Ryan King}]
  s.date = %q{2011-08-22}
  s.description = %q{A Ruby client for the Cassandra distributed database.}
  s.email = %q{}
  s.executables = [%q{cassandra_helper}]
  s.extra_rdoc_files = [%q{CHANGELOG}, %q{LICENSE}, %q{README.md}, %q{bin/cassandra_helper}, %q{lib/cassandra.rb}, %q{lib/cassandra/0.6.rb}, %q{lib/cassandra/0.6/cassandra.rb}, %q{lib/cassandra/0.6/columns.rb}, %q{lib/cassandra/0.6/protocol.rb}, %q{lib/cassandra/0.7.rb}, %q{lib/cassandra/0.7/cassandra.rb}, %q{lib/cassandra/0.7/columns.rb}, %q{lib/cassandra/0.7/protocol.rb}, %q{lib/cassandra/0.8.rb}, %q{lib/cassandra/0.8/cassandra.rb}, %q{lib/cassandra/0.8/columns.rb}, %q{lib/cassandra/0.8/protocol.rb}, %q{lib/cassandra/array.rb}, %q{lib/cassandra/cassandra.rb}, %q{lib/cassandra/column_family.rb}, %q{lib/cassandra/columns.rb}, %q{lib/cassandra/comparable.rb}, %q{lib/cassandra/constants.rb}, %q{lib/cassandra/debug.rb}, %q{lib/cassandra/helpers.rb}, %q{lib/cassandra/keyspace.rb}, %q{lib/cassandra/long.rb}, %q{lib/cassandra/mock.rb}, %q{lib/cassandra/ordered_hash.rb}, %q{lib/cassandra/protocol.rb}, %q{lib/cassandra/time.rb}]
  s.files = [%q{CHANGELOG}, %q{LICENSE}, %q{Manifest}, %q{README.md}, %q{Rakefile}, %q{bin/cassandra_helper}, %q{conf/0.6/cassandra.in.sh}, %q{conf/0.6/log4j.properties}, %q{conf/0.6/schema.json}, %q{conf/0.6/storage-conf.xml}, %q{conf/0.7/cassandra.in.sh}, %q{conf/0.7/cassandra.yaml}, %q{conf/0.7/log4j-server.properties}, %q{conf/0.7/schema.json}, %q{conf/0.7/schema.txt}, %q{conf/0.8/cassandra.in.sh}, %q{conf/0.8/cassandra.yaml}, %q{conf/0.8/log4j-server.properties}, %q{conf/0.8/schema.json}, %q{conf/0.8/schema.txt}, %q{lib/cassandra.rb}, %q{lib/cassandra/0.6.rb}, %q{lib/cassandra/0.6/cassandra.rb}, %q{lib/cassandra/0.6/columns.rb}, %q{lib/cassandra/0.6/protocol.rb}, %q{lib/cassandra/0.7.rb}, %q{lib/cassandra/0.7/cassandra.rb}, %q{lib/cassandra/0.7/columns.rb}, %q{lib/cassandra/0.7/protocol.rb}, %q{lib/cassandra/0.8.rb}, %q{lib/cassandra/0.8/cassandra.rb}, %q{lib/cassandra/0.8/columns.rb}, %q{lib/cassandra/0.8/protocol.rb}, %q{lib/cassandra/array.rb}, %q{lib/cassandra/cassandra.rb}, %q{lib/cassandra/column_family.rb}, %q{lib/cassandra/columns.rb}, %q{lib/cassandra/comparable.rb}, %q{lib/cassandra/constants.rb}, %q{lib/cassandra/debug.rb}, %q{lib/cassandra/helpers.rb}, %q{lib/cassandra/keyspace.rb}, %q{lib/cassandra/long.rb}, %q{lib/cassandra/mock.rb}, %q{lib/cassandra/ordered_hash.rb}, %q{lib/cassandra/protocol.rb}, %q{lib/cassandra/time.rb}, %q{test/cassandra_client_test.rb}, %q{test/cassandra_mock_test.rb}, %q{test/cassandra_test.rb}, %q{test/comparable_types_test.rb}, %q{test/eventmachine_test.rb}, %q{test/ordered_hash_test.rb}, %q{test/test_helper.rb}, %q{vendor/0.6/gen-rb/cassandra.rb}, %q{vendor/0.6/gen-rb/cassandra_constants.rb}, %q{vendor/0.6/gen-rb/cassandra_types.rb}, %q{vendor/0.7/gen-rb/cassandra.rb}, %q{vendor/0.7/gen-rb/cassandra_constants.rb}, %q{vendor/0.7/gen-rb/cassandra_types.rb}, %q{vendor/0.8/gen-rb/cassandra.rb}, %q{vendor/0.8/gen-rb/cassandra_constants.rb}, %q{vendor/0.8/gen-rb/cassandra_types.rb}, %q{cassandra.gemspec}]
  s.homepage = %q{http://fauna.github.com/fauna/cassandra/}
  s.rdoc_options = [%q{--line-numbers}, %q{--inline-source}, %q{--title}, %q{Cassandra}, %q{--main}, %q{README.md}]
  s.require_paths = [%q{lib}]
  s.rubyforge_project = %q{fauna}
  s.rubygems_version = %q{1.8.5}
  s.summary = %q{A Ruby client for the Cassandra distributed database.}
  s.test_files = [%q{test/cassandra_client_test.rb}, %q{test/cassandra_mock_test.rb}, %q{test/cassandra_test.rb}, %q{test/comparable_types_test.rb}, %q{test/eventmachine_test.rb}, %q{test/ordered_hash_test.rb}, %q{test/test_helper.rb}]

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<thrift_client>, [">= 0.7.0"])
      s.add_runtime_dependency(%q<json>, [">= 0"])
      s.add_runtime_dependency(%q<rake>, [">= 0"])
      s.add_runtime_dependency(%q<simple_uuid>, [">= 0.2.0"])
    else
      s.add_dependency(%q<thrift_client>, [">= 0.7.0"])
      s.add_dependency(%q<json>, [">= 0"])
      s.add_dependency(%q<rake>, [">= 0"])
      s.add_dependency(%q<simple_uuid>, [">= 0.2.0"])
    end
  else
    s.add_dependency(%q<thrift_client>, [">= 0.7.0"])
    s.add_dependency(%q<json>, [">= 0"])
    s.add_dependency(%q<rake>, [">= 0"])
    s.add_dependency(%q<simple_uuid>, [">= 0.2.0"])
  end
end
