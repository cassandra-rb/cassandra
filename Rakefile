unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver, Ryan King"
    p.project = "fauna"
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['thrift_client >=0.6.0', 'json', 'rake', 'simple_uuid >=0.1.0']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
  end
end

CassandraBinaries = {
  '0.6' => 'http://download.nextag.com/apache/cassandra/0.6.13/apache-cassandra-0.6.13-bin.tar.gz',
  '0.7' => 'http://www.trieuvan.com/apache/cassandra/0.7.4/apache-cassandra-0.7.4-bin.tar.gz',
  '0.8' => 'http://people.apache.org/~eevans/apache-cassandra-0.8.0-beta1-bin.tar.gz'
}

CASSANDRA_HOME = ENV['CASSANDRA_HOME'] || "#{ENV['HOME']}/cassandra"
CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '0.8'

directory CASSANDRA_HOME
directory File.join(CASSANDRA_HOME, 'test', 'data')

def setup_cassandra_version(version = CASSANDRA_VERSION)
  destination_directory = File.join(CASSANDRA_HOME, 'cassandra-' + CASSANDRA_VERSION)
  download_source       = CassandraBinaries[CASSANDRA_VERSION]
  download_destination  = File.join("/tmp", File.basename(download_source))

  unless File.exists?(File.join(destination_directory, 'bin','cassandra'))
    puts "downloading cassandra"
    sh "curl -L -o #{download_destination} #{download_source}"

    sh "tar xzf #{download_destination} -C #{CASSANDRA_HOME}"
    sh "mv #{destination_directory}-bin #{destination_directory}"
  end
end

desc "Start Cassandra"
task :cassandra => [:java, File.join(CASSANDRA_HOME, 'test', 'data')] do
  setup_cassandra_version

  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{File.expand_path(Dir.pwd)}/conf/cassandra-env.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME}/cassandra-#{CASSANDRA_VERSION} "
    env << "CASSANDRA_CONF=#{File.expand_path(Dir.pwd)}/conf/#{CASSANDRA_VERSION}"
  else
    env << "CASSANDRA_INCLUDE=#{ENV['CASSANDRA_INCLUDE']} "
    env << "CASSANDRA_HOME=#{ENV['CASSANDRA_HOME']} "
    env << "CASSANDRA_CONF=#{ENV['CASSANDRA_CONF']}"
  end

  Dir.chdir(File.join(CASSANDRA_HOME, 'server')) do
    sh("env #{env} bin/cassandra -f")
  end
end


desc "Check Java version"
task :java do
  unless `java -version 2>&1`.split("\n").first =~ /java version "1.6/ #"
    puts "You need to configure your environment for Java 1.6."
    puts "If you're on OS X, just export the following environment variables:"
    puts '  JAVA_HOME="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home"'
    puts '  PATH="/System/Library/Frameworks/JavaVM.framework/Versions/1.6/Home/bin:$PATH"'
    exit(1)
  end
end

namespace :data do
  desc "Reset test data"
  task :reset do
    puts "Resetting test data"
    sh("rm -rf #{File.join(CASSANDRA_HOME, 'server', 'data')}")
  end
end

# desc "Regenerate thrift bindings for Cassandra" # Dev only
task :thrift do
  puts "Generating Thrift bindings"
  system(
    "cd vendor &&
    rm -rf gen-rb &&
    thrift -gen rb #{CASSANDRA_HOME}/interface/cassandra.thrift")
end
