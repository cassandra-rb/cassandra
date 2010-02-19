unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver, Ryan King"
    p.project = "fauna"
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['thrift_client >= 0.4.0', 'json', 'rake', 'simple_uuid >= 0.1.0']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
    p.url = "http://blog.evanweaver.com/files/doc/fauna/cassandra/"
    p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
  end
end

REVISION = "298a0e66ba66c5d2a1e5d4a70f2f619ae3fbf72a"

PATCHES = []

CASSANDRA_HOME = "#{ENV['HOME']}/cassandra/server"

CASSANDRA_TEST = "#{ENV['HOME']}/cassandra/test"

GIT_REPO = "git://github.com/ryanking/cassandra.git"

directory CASSANDRA_TEST

desc "Start Cassandra"
task :cassandra => [:build_cassandra, CASSANDRA_TEST] do
  # Construct environment
  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{Dir.pwd}/conf/cassandra.in.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME} "
    env << "CASSANDRA_CONF=#{Dir.pwd}/conf"
  end
  # Start server
  Dir.chdir(CASSANDRA_TEST) do
    exec("env #{env} #{CASSANDRA_HOME}/bin/cassandra -f")
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

desc "Check Git version"
task :git do
  unless `git --version 2>&1` =~ /git version 1.(6|7)/
    puts "You need to install git 1.6 or 1.7"
    exit(1)
  end
end

desc "Checkout Cassandra from git"
task :clone_cassandra => [:java, :git] do
  # Like a git submodule, but all in one more obvious place
  unless File.exist?(CASSANDRA_HOME)
    puts "Checking Cassandra out from git"
    cmd = "git clone #{GIT_REPO} #{CASSANDRA_HOME}"
    if !system(cmd)
      put "Checkout failed. Try:\n  #{cmd}"
      exit(1)
    end
  end
end

desc "Check out the right revision"
task :checkout_cassandra => [:clone_cassandra] do
  Dir.chdir(CASSANDRA_HOME) do
    current_checkout = `git log | head -n1`
    if !current_checkout.include?(REVISION)
      puts "Updating Cassandra."
      system("rm -rf #{CASSANDRA_TEST}/data")
      system("ant clean && git fetch && git reset #{REVISION} --hard")
      # Delete untracked files
      Array(`git status`[/Untracked files:(.*)$/m, 1].to_s.split("\n")[3..-1]).each do |file|
        File.unlink(file.sub(/^.\s+/, "")) rescue nil
      end
    end
  end
end

desc "Rebuild Cassandra"
task :build_cassandra => [:checkout_cassandra] do
  unless File.exist?("#{CASSANDRA_HOME}/build")
    puts "Building Cassandra"
    cmd = "cd #{CASSANDRA_HOME} && ant"
    if !system(cmd)
      puts "Could not build Casssandra. Try:\n  #{cmd}"
      exit(1)
    end
  end
end

desc "Clean Cassandra build"
task :clean_cassandra do
  puts "Cleaning Cassandra"
  if File.exist?(CASSANDRA_HOME)
    Dir.chdir(CASSANDRA_HOME) do
      system("ant clean")
    end
  end
end

namespace :data do
  desc "Reset test data"
  task :reset do
    puts "Resetting test data"
    system("rm -rf #{CASSANDRA_TEST}/data")
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
