if (defined?(RUBY_ENGINE) && RUBY_ENGINE =~ /jruby/) or ENV['OS'] == "Windows_NT"
  File.open('Makefile', 'w'){|f| f.puts "all:\n\ninstall:\n" }
else
  require 'mkmf'

  $CFLAGS = "-g -O2 -Wall -Werror"

  create_makefile 'cassandra_native'
end
