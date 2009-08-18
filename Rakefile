
unless ENV['FROM_BIN_CASSANDRA_HELPER']
  require 'rubygems'
  require 'echoe'

  Echoe.new("cassandra") do |p|
    p.author = "Evan Weaver"
    p.project = "fauna"
    p.summary = "A Ruby client for the Cassandra distributed database."
    p.rubygems_version = ">= 0.8"
    p.dependencies = ['json', 'thrift']
    p.ignore_pattern = /^(data|vendor\/cassandra|cassandra|vendor\/thrift)/
    p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
    p.url = "http://blog.evanweaver.com/files/doc/fauna/cassandra/"
    p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
  end
end

REVISION = "15354b4906fd654d58fe50fd01ebf95b69434ba9"

PATCHES = [
  "http://issues.apache.org/jira/secure/attachment/12416014/0001-CASSANDRA-356-rename-clean-up-collectColumns-methods.txt",
  "http://issues.apache.org/jira/secure/attachment/12416073/0002-v3.patch",
  "http://issues.apache.org/jira/secure/attachment/12416074/357-v2.patch",
  "http://issues.apache.org/jira/secure/attachment/12416086/357-3.patch"]

CASSANDRA_HOME = "#{ENV['HOME']}/cassandra/r#{REVISION[0, 8]}"

CASSANDRA_TEST = "#{ENV['HOME']}/cassandra/test"

desc "Start Cassandra"
task :cassandra => [:checkout, :patch, :build] do
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

desc "Checkout Cassandra from git"
task :checkout do
  # Like a git submodule, but all in one obvious place
  unless File.exist?(CASSANDRA_HOME)
    system("git clone git://git.apache.org/cassandra.git #{CASSANDRA_HOME}")
    ENV["RESET"] = "true"
  end
end

desc "Apply patches to Cassandra checkout"
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
        raise "#{url} failed" unless system("curl #{url} | patch -p1")
        system("git commit -a -m 'Applied patch: #{url.inspect}'")
      end
    end
  end
end

desc "Rebuild Cassandra"
task :build do
  Dir.chdir(CASSANDRA_HOME) { system("ant") } unless File.exist?("#{CASSANDRA_HOME}/build")
end

desc "Clean Cassandra build"
task :clean do
  Dir.chdir(CASSANDRA_HOME) { system("ant clean") } if File.exist?(CASSANDRA_HOME)
end

namespace :data do
  desc "Reset test data"
  task :reset do
    system("rm -rf #{CASSANDRA_TEST}/data")
  end
end

desc "Regenerate thrift bindings for Cassandra"
task :thrift do
  system(
    "cd vendor &&
    rm -rf gen-rb &&
    thrift -gen rb #{CASSANDRA_HOME}/interface/cassandra.thrift")
end

