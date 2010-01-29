
require 'rubygems'
require 'echoe'

Echoe.new("thrift_client") do |p|
  p.author = "Evan Weaver"
  p.project = "fauna"
  p.summary = "A Thrift client wrapper that encapsulates some common failover behavior."
  p.rubygems_version = ">= 0.8"
  p.dependencies = ['thrift']
  p.ignore_pattern = /^(vendor\/thrift)/
  p.rdoc_pattern = /^(lib|bin|tasks|ext)|^README|^CHANGELOG|^TODO|^LICENSE|^COPYING$/
  p.url = "http://blog.evanweaver.com/files/doc/fauna/thrift_client/"
  p.docs_host = "blog.evanweaver.com:~/www/bax/public/files/doc/"
  p.spec_pattern = "spec/*_spec.rb"
end

desc "Start Scribe in order to run the system tests"
task :start do
  system("mkdir /tmp/scribetest/") unless File.exist?("/tmp/scribetest/")
  system("scribed -c #{File.expand_path(File.dirname(__FILE__))}/test/scribe.conf &")
end

desc "Stop Scribe"
task :stop do
  system("killall scribed")
end

desc "Restart Scribe"
task :restart => ["stop", "start"] do
end

task :test => ["restart"]
