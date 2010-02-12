
require "#{File.dirname(__FILE__)}/test_helper"

class SimpleTest < Test::Unit::TestCase

  S = ThriftClient::Simple
  S.make_struct("Example", S::Field.new(:name, S::STRING, 1))
  S.make_struct("Args")
  S.make_struct("Retval", S::Field.new(:rv, S::I32, 0))

  def test_definition
    assert Struct::ST_Example
    assert Struct::ST_Args
    assert Struct::ST_Retval
  end

  ## Encoding

  def test_boolean_encoding
    assert_equal "\001", S.pack_value(S::BOOL, true)
    assert_equal "\000", S.pack_value(S::BOOL, false)
  end

  def test_byte_encoding
    assert_equal "\xc7", S.pack_value(S::BYTE, 199)
  end

  def test_i16_encoding
    assert_equal "\x00\x96", S.pack_value(S::I16, 150)
  end

  def test_i32_encoding
    assert_equal "\x00\x96\xb4\x3f", S.pack_value(S::I32, 9876543)
  end

  def test_i64_encoding
    assert_equal "\x00\x00\x00\x1c\xbb\xf3\x09\x04", S.pack_value(S::I64, 123412351236)
  end

  def test_double_encoding
    assert_equal "\x40\x23\x00\x00\x00\x00\x00\x00", S.pack_value(S::DOUBLE, 9.5)
  end

  def test_string_encoding
    assert_equal "\x00\x00\x00\x05hello", S.pack_value(S::STRING, "hello")
  end

  def test_list_encoding
    assert_equal "\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15", 
      S.pack_value(S::ListType.new(S::I32), [ 23, 22, 21 ])
  end

  def test_map_encoding
    assert_equal "\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05", 
      S.pack_value(S::MapType.new(S::STRING, S::I32), "cat" => 5)
  end

  def test_set_encoding
    assert_equal "\x08\x00\x00\x00\x01\x00\x00\x00\x04", 
      S.pack_value(S::SetType.new(S::I32), [ 4 ])
  end

  def test_struct_encoding
    assert_equal "\x0b\x00\x01\x00\x00\x00\x06Commie\x00", 
      S.pack_value(S::StructType.new(Struct::ST_Example), Struct::ST_Example.new("Commie"))
  end

  def test_request_encoding
    assert_equal "\x80\x01\x00\x01\x00\x00\x00\x09getHeight\x00\x00\x00\x17\x00", 
      S.pack_request("getHeight", Struct::ST_Args.new, 23)
  end

  ## Decoding

  def test_boolean_decoding
    assert_equal true, S.read_value(StringIO.new("\x01"), S::BOOL)
    assert_equal false, S.read_value(StringIO.new("\x00"), S::BOOL)
  end

  def test_byte_decoding
    assert_equal -57, S.read_value(StringIO.new("\xc7"), S::BYTE)
  end

  def test_i16_decoding
    assert_equal 150, S.read_value(StringIO.new("\x00\x96"), S::I16)
  end

  def test_i32_decoding
    assert_equal 9876543, S.read_value(StringIO.new("\x00\x96\xb4\x3f"), S::I32)
  end

  def test_i64_decoding
    assert_equal 123412351236, 
      S.read_value(StringIO.new("\x00\x00\x00\x1c\xbb\xf3\x09\x04"), S::I64)
  end

  def test_double_decoding
    assert_equal 9.5, 
      S.read_value(StringIO.new("\x40\x23\x00\x00\x00\x00\x00\x00"), S::DOUBLE)
  end

  def test_string_decoding
    assert_equal "hello", S.read_value(StringIO.new("\x00\x00\x00\x05hello"), S::STRING)
  end

  def test_list_decoding
    assert_equal [ 23, 22, 21 ], 
      S.read_value(StringIO.new("\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15"), 
      S::ListType.new(S::I32))
  end

  def test_map_decoding
    assert_equal({ "cat" => 5 }, 
      S.read_value(StringIO.new("\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05"), 
      S::MapType.new(S::STRING, S::I32)))
  end

  def test_set_decoding
    assert_equal [ 4 ], 
      S.read_value(StringIO.new("\x08\x00\x00\x00\x01\x00\x00\x00\x04"), 
      S::ListType.new(S::I32))
  end

  def test_struct_decoding
    assert_equal Struct::ST_Example.new("Commie"), 
      S.read_value(StringIO.new("\x0b\x00\x01\x00\x00\x00\x06Commie\x00"), 
      S::StructType.new(Struct::ST_Example))
  end

  def test_response_decoding
    assert_equal [ "getHeight", 255, 1 ], 
      S.read_response(
      StringIO.new("\x80\x01\x00\x02\x00\x00\x00\x09getHeight\x00\x00\x00\xff\x08\x00\x00\x00\x00\x00\x01\x00"),
      Struct::ST_Retval)
  end

end
