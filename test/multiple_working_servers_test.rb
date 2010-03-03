require "#{File.dirname(__FILE__)}/test_helper"

class MultipleWorkingServersTest < Test::Unit::TestCase
  def setup
    @servers = ["127.0.0.1:1461", "127.0.0.1:1462", "127.0.0.1:1463"]
    @socket = 1461
    @timeout = 0.2
    @options = {:protocol_extra_params => [false]}
    @pids = []
    @servers.each do |s|
      @pids << Process.fork do
        Signal.trap("INT") { exit }
        Greeter::Server.new(s.split(':').last).serve
      end
    end
    # Need to give the child process a moment to open the listening socket or
    # we get occasional "could not connect" errors in tests.
    sleep 0.05
  end

  def teardown
    @pids.each do |pid|
      Process.kill("INT", pid)
      Process.wait(pid)
    end
  end

  def test_server_creates_new_client_that_can_talk_to_all_servers_after_disconnect
    client = ThriftClient.new(Greeter::Client, @servers, @options)
    client.greeting("someone")
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client # Sanity check

    client.disconnect!
    client.greeting("someone")
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client

    # Moves on to the second server
    assert_nothing_raised {
      client.greeting("someone")
      client.greeting("someone")
    }
  end

  def test_server_doesnt_max_out_after_explicit_disconnect
    client = ThriftClient.new(Greeter::Client, @servers, @options.merge(:server_max_requests => 2))
    client.greeting("someone")
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client # Sanity check

    client.disconnect!

    client.greeting("someone")
    internal_client = client.client
    client.greeting("someone")
    assert_equal internal_client, client.client, "ThriftClient should not have reset the internal client if the counter was reset on disconnect"
  end

  def test_server_disconnect_doesnt_drop_servers_with_retry_period
    client = ThriftClient.new(Greeter::Client, @servers, @options.merge(:server_max_requests => 2, :retry_period => 1))
    3.times {
      client.greeting("someone")
      internal_client = client.client
      client.greeting("someone")
      assert_equal internal_client, client.client # Sanity check

      client.disconnect!

      client.greeting("someone")
      internal_client = client.client
      client.greeting("someone")
      assert_equal internal_client, client.client, "ThriftClient should not have reset the internal client if the counter was reset on disconnect"
    }
  end


  def test_server_max_requests
    client = ThriftClient.new(Greeter::Client, @servers, @options.merge(:server_max_requests => 2))

    client.greeting("someone")
    internal_client = client.client

    client.greeting("someone")
    assert_equal internal_client, client.client

    # This next call maxes out the requests for that "client" object
    # and moves on to the next.
    client.greeting("someone")
    assert_not_equal internal_client, new_client = client.client

    # And here we should still have the same client as the last one...
    client.greeting("someone")
    assert_equal new_client, client.client

    # Until we max it out, too.
    client.greeting("someone")
    assert_not_equal new_client, client.client
    assert_not_nil client.client

    new_new_client = client.client
    # And we should still have one server left
    client.greeting("someone")
    assert_equal new_new_client, client.client
  end
end
