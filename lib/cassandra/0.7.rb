class Cassandra
  def self.VERSION
    "0.7"
  end
end

require "#{File.expand_path(File.dirname(__FILE__))}/../cassandra"