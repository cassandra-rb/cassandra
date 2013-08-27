require File.expand_path(File.dirname(__FILE__) + '/test_helper')

class CompositeTypesTest < Test::Unit::TestCase
  include Cassandra::Constants

  def setup
    @col_parts = [[363].pack('N'), 'extradites-mulling', SimpleUUID::UUID.new().bytes]
    @col = Cassandra::Composite.new(*@col_parts)

    @part0_length = 2 + 4 + 1 # size + int + end_term
    @part1_length = 2 + @col_parts[1].length + 1 # size + string_len + end_term
    @part2_length = 2 + @col_parts[2].length + 1 # size + uuid_bytes + end_term

    @types = ['IntegerType', 'UTF8Type', 'TimeUUIDType']
    @type_aliaes = ['i', 's', 't']

    @dycol = Cassandra::DynamicComposite.new(*@types.zip(@col_parts))
    @dycol_alias = Cassandra::DynamicComposite.new(*@type_aliaes.zip(@col_parts))
  end

  def test_creation_from_parts
    (0..2).each do |i|
      assert_equal(@col_parts[i], @col[i])
      assert_equal(@col_parts[i], @dycol[i])
      assert_equal(@col_parts[i], @dycol_alias[i])
    end

    col2 = Cassandra::Composite.new_from_parts(@col_parts)
    (0..2).each do |i|
      assert_equal(@col_parts[i], col2[i].to_s)
    end
  end

  def test_packing_and_unpacking
    assert_equal(@part0_length + @part1_length + @part2_length, @col.pack.length)

    col2 = Cassandra::Composite.new_from_packed(@col.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@col, col2)
    # make sure we set this while we have the packed string handy
    assert_equal(@col.pack.hash, col2.instance_variable_get(:@hash))

    col2 = Cassandra::Composite.new(@col.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@col, col2)
  end

  def test_packing_and_unpacking_dynamic_columns
    part0_length = @part0_length + 2 + @types[0].length
    part1_length = @part1_length + 2 + @types[1].length
    part2_length = @part2_length + 2 + @types[2].length
    assert_equal(part0_length + part1_length + part2_length, @dycol.pack.length)

    col2 = Cassandra::DynamicComposite.new_from_packed(@dycol.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@dycol, col2)
    # make sure we set this while we have the packed string handy
    assert_equal(@dycol.pack.hash, col2.instance_variable_get(:@hash))

    col2 = Cassandra::DynamicComposite.new(@dycol.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@dycol, col2)
  end

  def test_packing_and_unpacking_dynamic_columns_with_aliases
    part0_length = @part0_length + 2
    part1_length = @part1_length + 2
    part2_length = @part2_length + 2
    assert_equal(part0_length + part1_length + part2_length, @dycol_alias.pack.length)

    col2 = Cassandra::DynamicComposite.new_from_packed(@dycol_alias.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@dycol_alias, col2)

    col2 = Cassandra::DynamicComposite.new(@dycol_alias.pack)
    assert_equal(@col_parts[0], col2[0])
    assert_equal(@col_parts[1], col2[1])
    assert_equal(@col_parts[2], col2[2])
    assert_equal(@dycol_alias, col2)
  end
end
