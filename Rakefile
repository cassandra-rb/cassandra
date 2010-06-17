unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver, Ryan King"
    p.project = "fauna"
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['thrift_client >=0.4.0', 'json', 'rake', 'simple_uuid >=0.1.0']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
    p.url = "http://blog.evanweaver.com/files/doc/fauna/cassandra/"
    p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
  end
end

CASSANDRA_HOME = ENV['CASSANDRA_HOME'] || "#{ENV['HOME']}/cassandra"
DOWNLOAD_DIR = "/tmp"
DIST_URL = "http://archive.apache.org/dist/cassandra/0.6.1/apache-cassandra-0.6.1-bin.tar.gz"
DIST_FILE = DIST_URL.split('/').last

directory CASSANDRA_HOME
directory File.join(CASSANDRA_HOME, 'test', 'data')

desc "Start Cassandra"
task :cassandra => [:java, File.join(CASSANDRA_HOME, 'server'), File.join(CASSANDRA_HOME, 'test', 'data')] do
  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{File.expand_path(Dir.pwd)}/conf/cassandra.in.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME}/server "
    env << "CASSANDRA_CONF=#{File.expand_path(Dir.pwd)}/conf"
  else
    env << "CASSANDRA_INCLUDE=#{ENV['CASSANDRA_INCLUDE']} "
    env << "CASSANDRA_HOME=#{ENV['CASSANDRA_HOME']} "
    env << "CASSANDRA_CONF=#{ENV['CASSANDRA_CONF']}"
  end

  Dir.chdir(File.join(CASSANDRA_HOME, 'server')) do
    sh("env #{env} bin/cassandra -f")
  end
end

file File.join(CASSANDRA_HOME, 'server') => File.join(DOWNLOAD_DIR, DIST_FILE) do
  Dir.chdir(CASSANDRA_HOME) do
    sh "tar xzf #{File.join(DOWNLOAD_DIR, DIST_FILE)} -C #{CASSANDRA_HOME}"
    sh "mv #{DIST_FILE.split('.')[0..2].join('.').sub('-bin', '')} server"
  end
end

file File.join(DOWNLOAD_DIR, DIST_FILE) => CASSANDRA_HOME do
  puts "downloading"
  cmd = "curl -L -o #{File.join(DOWNLOAD_DIR, DIST_FILE)} #{DIST_URL}"
  sh cmd
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
