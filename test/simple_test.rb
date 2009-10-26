
require "#{File.dirname(__FILE__)}/test_helper"

class SimpleTest < Test::Unit::TestCase

  ThriftClient::Simple.make_struct("Example", ThriftClient::Simple::Field.new(:name, ThriftClient::Simple::STRING, 1))
  ThriftClient::Simple.make_struct("Args")
  ThriftClient::Simple.make_struct("Retval", ThriftClient::Simple::Field.new(:rv, ThriftClient::Simple::I32, 0))
 
  def test_definition
    assert Struct::ST_Example
    assert Struct::ST_Args
    assert Struct::ST_Retval
  end
  
  ## Encoding

  def test_boolean_encoding
    assert_equal "\001", ThriftClient::Simple.pack_value(ThriftClient::Simple::BOOL, true)
    assert_equal "\000", ThriftClient::Simple.pack_value(ThriftClient::Simple::BOOL, false)
  end

  def test_byte_encoding
    assert_equal "\xc7", ThriftClient::Simple.pack_value(ThriftClient::Simple::BYTE, 199)
  end

  def test_i16_encoding
    assert_equal "\x00\x96", ThriftClient::Simple.pack_value(ThriftClient::Simple::I16, 150)
  end

  def test_i32_encoding
    assert_equal "\x00\x96\xb4\x3f", ThriftClient::Simple.pack_value(ThriftClient::Simple::I32, 9876543)
  end

  def test_i64_encoding
    assert_equal "\x00\x00\x00\x1c\xbb\xf3\x09\x04", ThriftClient::Simple.pack_value(ThriftClient::Simple::I64, 123412351236)
  end

  def test_double_encoding
    assert_equal "\x40\x23\x00\x00\x00\x00\x00\x00", ThriftClient::Simple.pack_value(ThriftClient::Simple::DOUBLE, 9.5)
  end

  def test_string_encoding
    assert_equal "\x00\x00\x00\x05hello", ThriftClient::Simple.pack_value(ThriftClient::Simple::STRING, "hello")
  end

  def test_list_encoding
    assert_equal "\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15", ThriftClient::Simple.pack_value(ThriftClient::Simple::ListType.new(ThriftClient::Simple::I32), [ 23, 22, 21 ])
  end

  def test_map_encoding
    assert_equal "\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05", ThriftClient::Simple.pack_value(ThriftClient::Simple::MapType.new(ThriftClient::Simple::STRING, ThriftClient::Simple::I32), "cat" => 5)
  end

  def test_set_encoding
    assert_equal "\x08\x00\x00\x00\x01\x00\x00\x00\x04", ThriftClient::Simple.pack_value(ThriftClient::Simple::SetType.new(ThriftClient::Simple::I32), [ 4 ])
  end

  def test_struct_encoding
    assert_equal "\x0b\x00\x01\x00\x00\x00\x06Commie\x00", ThriftClient::Simple.pack_value(ThriftClient::Simple::StructType.new(Struct::ST_Example), Struct::ST_Example.new("Commie"))
  end

  def test_request_encoding
    assert_equal "\x80\x01\x00\x01\x00\x00\x00\x09getHeight\x00\x00\x00\x17\x00", ThriftClient::Simple.pack_request("getHeight", Struct::ST_Args.new, 23)
  end
  
  ## Decoding

  def test_boolean_decoding
    assert_equal  true, ThriftClient::Simple.read_value(StringIO.new("\x01"), ThriftClient::Simple::BOOL)
    assert_equal  false, ThriftClient::Simple.read_value(StringIO.new("\x00"), ThriftClient::Simple::BOOL)
  end

  def test_byte_decoding
    assert_equal  -57, ThriftClient::Simple.read_value(StringIO.new("\xc7"), ThriftClient::Simple::BYTE)
  end

  def test_i16_decoding
    assert_equal  150, ThriftClient::Simple.read_value(StringIO.new("\x00\x96"), ThriftClient::Simple::I16)
  end

  def test_i32_decoding
    assert_equal  9876543, ThriftClient::Simple.read_value(StringIO.new("\x00\x96\xb4\x3f"), ThriftClient::Simple::I32)
  end

  def test_i64_decoding
    assert_equal  123412351236, ThriftClient::Simple.read_value(StringIO.new("\x00\x00\x00\x1c\xbb\xf3\x09\x04"), ThriftClient::Simple::I64)
  end

  def test_double_decoding
    assert_equal  9.5, ThriftClient::Simple.read_value(StringIO.new("\x40\x23\x00\x00\x00\x00\x00\x00"), ThriftClient::Simple::DOUBLE)
  end

  def test_string_decoding
    assert_equal "hello", ThriftClient::Simple.read_value(StringIO.new("\x00\x00\x00\x05hello"), ThriftClient::Simple::STRING)
  end

  def test_list_decoding
    assert_equal  [ 23, 22, 21 ], ThriftClient::Simple.read_value(StringIO.new("\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15"), ThriftClient::Simple::ListType.new(ThriftClient::Simple::I32))
    end

  def test_map_decoding
    assert_equal({ "cat" => 5 }, ThriftClient::Simple.read_value(StringIO.new("\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05"), ThriftClient::Simple::MapType.new(ThriftClient::Simple::STRING, ThriftClient::Simple::I32)))
  end

  def test_set_decoding
    assert_equal  [ 4 ], ThriftClient::Simple.read_value(StringIO.new("\x08\x00\x00\x00\x01\x00\x00\x00\x04"), ThriftClient::Simple::ListType.new(ThriftClient::Simple::I32))
  end

  def test_struct_decoding
    assert_equal  Struct::ST_Example.new("Commie"), ThriftClient::Simple.read_value(StringIO.new("\x0b\x00\x01\x00\x00\x00\x06Commie\x00"), ThriftClient::Simple::StructType.new(Struct::ST_Example))
  end

  def test_response_decoding
    assert_equal  [ "getHeight", 255, 1 ], ThriftClient::Simple.read_response(StringIO.new("\x80\x01\x00\x02\x00\x00\x00\x09getHeight\x00\x00\x00\xff\x08\x00\x00\x00\x00\x00\x01\x00"), Struct::ST_Retval)
  end
end
