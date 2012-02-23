require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class CompositeTypesTest < Test::Unit::TestCase
  include Cassandra::Constants

  def setup
    @col_parts = [[363].pack('N'), 'extradites-mulling', SimpleUUID::UUID.new().bytes]
    @col = Cassandra::Composite.new(*@col_parts)
  end

  def test_creation_from_parts
    assert_equal(@col_parts[0], @col[0])
    assert_equal(@col_parts[1], @col[1])
    assert_equal(@col_parts[2], @col[2])
  end

  def test_packing_and_unpacking
    part0_length = 2 + 4 + 1 # size + int + end_term
    part1_length = 2 + @col_parts[1].length + 1 # size + string_len + end_term
    part2_length = 2 + @col_parts[2].length + 1 # size + uuid_bytes + end_term
    assert_equal(part0_length + part1_length + part2_length, @col.pack.length)

    col2 = Cassandra::Composite.new(@col.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@col, col2)
  end
end
