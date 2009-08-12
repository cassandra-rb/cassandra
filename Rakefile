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

desc "Start Cassandra"
task :cassandra => [:checkout, :patch, :build] do
  env = "CASSANDRA_INCLUDE=#{Dir.pwd}/conf/cassandra.in.sh" unless ENV["CASSANDRA_INCLUDE"]
  exec("env #{env} cassandra/bin/cassandra -f")
end

REVISION = "15354b4906fd654d58fe50fd01ebf95b69434ba9"
PATCHES = [
  "http://issues.apache.org/jira/secure/attachment/12416014/0001-CASSANDRA-356-rename-clean-up-collectColumns-methods.txt",
  "https://issues.apache.org/jira/secure/attachment/12416073/0002-v3.patch",
  "http://issues.apache.org/jira/secure/attachment/12416074/357-v2.patch",
  "http://issues.apache.org/jira/secure/attachment/12416086/357-3.patch"]
  
task :checkout do
  # Like a git submodule, but all in one obvious place
  unless File.exist?("cassandra")
    system("git clone git://git.apache.org/cassandra.git") 
    ENV["RESET"] = "true"
  end
end

task :patch do
  if ENV["RESET"]
    Dir.chdir("cassandra") do
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

task :build do
  Dir.chdir("cassandra") { system("ant") } unless File.exist?("cassandra/build")
end

task :clean do
  Dir.chdir("cassandra") { system("ant clean") }
end

desc "Regenerate thrift bindings for Cassandra"
task :thrift do
  system(
    "cd vendor &&
    rm -rf gen-rb &&
    thrift -gen rb ../cassandra/interface/cassandra.thrift")
end

