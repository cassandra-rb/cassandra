require File.expand_path(File.dirname(__FILE__) + '/test_helper')

if RUBY_VERSION < '1.9' || CASSANDRA_VERSION == '0.6'
  puts "Skipping EventMachine test"
else

  require 'thrift_client/event_machine'

  class EventmachineTest < Test::Unit::TestCase

    def test_twitter
      @twitter = Cassandra.new('Twitter', "127.0.0.1:9160", :retries => 2, :exception_classes => [], :transport => Thrift::EventMachineTransport, :transport_wrapper => nil)
      @twitter.clear_keyspace!
    end

    private

    def em_test(name)
      EM.run do
        Fiber.new do
          begin
            send("raw_#{name}".to_sym)
          ensure
            EM.stop
          end
        end.resume
      end
    end

    def self.wrap_tests
      self.public_instance_methods.select { |m| m =~ /^test_/ }.each do |meth|
        alias_method :"raw_#{meth}", meth
        define_method(meth) do
          em_test(meth)
        end
      end
    end

    wrap_tests

  end
end
