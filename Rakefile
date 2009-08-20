
unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver"
    p.project = "fauna"
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['thrift', 'rake']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
    p.url = "http://blog.evanweaver.com/files/doc/fauna/cassandra/"
    p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
  end
end

REVISION = "e959b2c7f6d78b51492c5e7b19beb30c36e75987"

PATCHES = []

CASSANDRA_HOME = "#{ENV['HOME']}/cassandra/r#{REVISION[0, 8]}"

CASSANDRA_TEST = "#{ENV['HOME']}/cassandra/test"

desc "Start Cassandra"
task :cassandra => [:java, :checkout, :patch, :build] do
  # Construct environment
  env = ""
  if !ENV["CASSANDRA_INCLUDE"]
    env << "CASSANDRA_INCLUDE=#{Dir.pwd}/conf/cassandra.in.sh "
    env << "CASSANDRA_HOME=#{CASSANDRA_HOME} "
    env << "CASSANDRA_CONF=#{File.expand_path(File.dirname(__FILE__))}/conf" 
  end  
  # Create data dir
  Dir.mkdir(CASSANDRA_TEST) if !File.exist?(CASSANDRA_TEST)
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

desc "Checkout Cassandra from git"
task :checkout do
  # Check git version
  unless `git --version 2>&1` =~ /git version 1.6/
    puts "You need to install git 1.6."
    exit(1)
  end
  # Like a git submodule, but all in one more obvious place
  unless File.exist?(CASSANDRA_HOME)
    cmd = "git clone git://git.apache.org/cassandra.git #{CASSANDRA_HOME}"
    if !system(cmd)
      put "Checkout failed. Try:\n  #{cmd}"
      exit(1)
    end
    ENV["RESET"] = "true"
  end
end

desc "Apply patches to Cassandra checkout; use RESET=1 to force"
task :patch do
  if ENV["RESET"]
    system("rm -rf #{CASSANDRA_TEST}/data")
    Dir.chdir(CASSANDRA_HOME) do
      system("ant clean && git fetch && git reset #{REVISION} --hard")
      # Delete untracked files, so that the patchs can apply again
      Array(`git status`[/Untracked files:(.*)$/m, 1].to_s.split("\n")[3..-1]).each do |file|
        File.unlink(file.sub(/^.\s+/, "")) rescue nil
      end
      # Patch, with a handy commit for each one
      PATCHES.each do |url|
        raise "#{url} failed" unless system("wget -O - #{url} | patch -p1")
        system("git commit -a -m 'Applied patch: #{url.inspect}'")
      end
    end
  end
end

desc "Rebuild Cassandra"
task :build do
  unless File.exist?("#{CASSANDRA_HOME}/build")
    cmd = "cd #{CASSANDRA_HOME} && ant"
    if !system(cmd)
      puts "Could not build Casssandra. Try:\n  #{cmd}"
      exit(1)
    end
  end
end

desc "Clean Cassandra build"
task :clean do
  if File.exist?(CASSANDRA_HOME)
    Dir.chdir(CASSANDRA_HOME) do
      system("ant clean")
    end
  end      
end

namespace :data do
  desc "Reset test data"
  task :reset do
    system("rm -rf #{CASSANDRA_TEST}/data")
  end
end

# desc "Regenerate thrift bindings for Cassandra" # Dev only
task :thrift do
  system(
    "cd vendor &&
    rm -rf gen-rb &&
    thrift -gen rb #{CASSANDRA_HOME}/interface/cassandra.thrift")
end

