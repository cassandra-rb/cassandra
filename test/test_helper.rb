
require 'test/unit'
require 'benchmark'
$LOAD_PATH << "#{File.expand_path(File.dirname(__FILE__))}/../lib"
require 'thrift_client'
require 'thrift_client/simple'

$LOAD_PATH << File.expand_path(File.dirname(__FILE__)) 
require 'greeter/greeter'
require 'greeter/server'

begin; require 'ruby-debug'; rescue LoadError; end
