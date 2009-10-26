
require 'test/unit'
$LOAD_PATH << "#{File.expand_path(File.dirname(__FILE__))}/../lib"
require 'thrift_client'
require 'thrift_client/simple'
require 'scribe'
begin; require 'ruby-debug'; rescue LoadError; end
