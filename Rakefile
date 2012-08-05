require 'fileutils'

unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver, Ryan King"
    p.url = 'http://github.com/twitter/cassandra'
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['thrift_client >=0.7.0 <0.9', 'json', 'rake', 'simple_uuid ~>0.2.0']
    p.development_dependencies = ['echoe']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift|.*\.rbc)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
    p.retain_gemspec = true
  end
end

CassandraBinaries = {
  '0.6' => 'http://archive.apache.org/dist/cassandra/0.6.13/apache-cassandra-0.6.13-bin.tar.gz',
  '0.7' => 'http://archive.apache.org/dist/cassandra/0.7.9/apache-cassandra-0.7.9-bin.tar.gz',
  '0.8' => 'http://archive.apache.org/dist/cassandra/0.8.7/apache-cassandra-0.8.7-bin.tar.gz',
  '1.0' => 'http://archive.apache.org/dist/cassandra/1.0.6/apache-cassandra-1.0.6-bin.tar.gz'
}

CASSANDRA_HOME = ENV['CASSANDRA_HOME'] || "#{ENV['HOME']}/cassandra"
CASSANDRA_VERSION = ENV['CASSANDRA_VERSION'] || '0.8'
CASSANDRA_PIDFILE = ENV['CASSANDRA_PIDFILE'] || "#{CASSANDRA_HOME}/cassandra.pid"

def setup_cassandra_version(version = CASSANDRA_VERSION)
  FileUtils.mkdir_p CASSANDRA_HOME

  destination_directory = File.join(CASSANDRA_HOME, 'cassandra-' + CASSANDRA_VERSION)

  unless File.exists?(File.join(destination_directory, 'bin','cassandra'))
    download_source       = CassandraBinaries[CASSANDRA_VERSION]
    download_destination  = File.join("/tmp", File.basename(download_source))
    untar_directory       = File.join(CASSANDRA_HOME,  File.basename(download_source,'-bin.tar.gz'))

    puts "downloading cassandra"
    sh "curl -L -o #{download_destination} #{download_source}"

    sh "tar xzf #{download_destination} -C #{CASSANDRA_HOME}"
    sh "mv #{untar_directory} #{destination_directory}"
  end
end

def setup_environment
  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{File.expand_path(Dir.pwd)}/conf/#{CASSANDRA_VERSION}/cassandra.in.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME}/cassandra-#{CASSANDRA_VERSION} "
    env << "CASSANDRA_CONF=#{File.expand_path(Dir.pwd)}/conf/#{CASSANDRA_VERSION}"
  else
    env << "CASSANDRA_INCLUDE=#{ENV['CASSANDRA_INCLUDE']} "
    env << "CASSANDRA_HOME=#{ENV['CASSANDRA_HOME']} "
    env << "CASSANDRA_CONF=#{ENV['CASSANDRA_CONF']}"
  end

  env
end

def running?(pid_file = nil)
  pid_file ||= CASSANDRA_PIDFILE

  if File.exists?(pid_file)
    pid = File.new(pid_file).read.to_i
    begin
      Process.kill(0, pid)
      return true
    rescue
      File.delete(pid_file)
    end
  end

  false
end

namespace :cassandra do
  desc "Start Cassandra"
  task :start, [:daemonize] => :java do |t, args|
    args.with_defaults(:daemonize => true)

    setup_cassandra_version

    env = setup_environment

    Dir.chdir(File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")) do
      sh("env #{env} bin/cassandra #{'-f' unless args.daemonize} -p #{CASSANDRA_PIDFILE}")
    end
  end

  desc "Stop Cassandra"
  task :stop => :java do
    setup_cassandra_version
    env = setup_environment
    sh("kill $(cat #{CASSANDRA_PIDFILE})")
  end
end

desc "Start Cassandra"
task :cassandra => :java do
  begin
    Rake::Task["cassandra:start"].invoke(false)
  rescue RuntimeError => e
    raise e unless e.message =~ /Command failed with status \(130\)/ # handle keyboard interupt errors
  end
end

desc "Run the Cassandra CLI"
task :cli do
  Dir.chdir(File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")) do
    sh("bin/cassandra-cli -host localhost -port 9160")
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
    sh("rm -rf #{File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}", 'data')}")
  end

  desc "Load test data structures."
  task :load do
    unless CASSANDRA_VERSION == '0.6'

      schema_path = "#{File.expand_path(Dir.pwd)}/conf/#{CASSANDRA_VERSION}/schema.txt"
      puts "Loading test data structures."
      Dir.chdir(File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")) do
        begin
          sh("bin/cassandra-cli --host localhost --batch < #{schema_path}")
        rescue
          puts "Schema already loaded."
        end
      end
    end
  end
end

task :test => 'data:load'

# desc "Regenerate thrift bindings for Cassandra" # Dev only
task :thrift do
  puts "Generating Thrift bindings"
  FileUtils.mkdir_p "vendor/#{CASSANDRA_VERSION}"

  system(
    "cd vendor/#{CASSANDRA_VERSION} &&
    rm -rf gen-rb &&
    thrift -gen rb #{File.join(CASSANDRA_HOME, "cassandra-#{CASSANDRA_VERSION}")}/interface/cassandra.thrift")
end

task :fix_perms do
  chmod_R 0755, './'
end

task :pkg => [:fix_perms]
