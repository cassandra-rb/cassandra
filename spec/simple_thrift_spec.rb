
require "lib/simple_thrift"

describe "SimpleThrift" do
  struct = SimpleThrift.make_struct("Example", SimpleThrift::Field.new(:name, SimpleThrift::STRING, 1))
  arg_struct = SimpleThrift.make_struct("Args")
  rv_struct = SimpleThrift.make_struct("Retval", SimpleThrift::Field.new(:rv, SimpleThrift::I32, 0))

  context "encodes" do
    it "boolean" do
      SimpleThrift.pack_value(SimpleThrift::BOOL, true).should == "\001"
      SimpleThrift.pack_value(SimpleThrift::BOOL, false).should == "\000"
    end

    it "byte" do
      SimpleThrift.pack_value(SimpleThrift::BYTE, 199).should == "\xc7"
    end

    it "i16" do
      SimpleThrift.pack_value(SimpleThrift::I16, 150).should == "\x00\x96"
    end

    it "i32" do
      SimpleThrift.pack_value(SimpleThrift::I32, 9876543).should == "\x00\x96\xb4\x3f"
    end

    it "i64" do
      SimpleThrift.pack_value(SimpleThrift::I64, 123412351236).should == "\x00\x00\x00\x1c\xbb\xf3\x09\x04"
    end

    it "double" do
      SimpleThrift.pack_value(SimpleThrift::DOUBLE, 9.5).should == "\x40\x23\x00\x00\x00\x00\x00\x00"
    end

    it "string" do
      SimpleThrift.pack_value(SimpleThrift::STRING, "hello").should == "\x00\x00\x00\x05hello"
    end

    it "list" do
      SimpleThrift.pack_value(SimpleThrift::ListType.new(SimpleThrift::I32), [ 23, 22, 21 ]).should == "\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15"
    end

    it "map" do
      SimpleThrift.pack_value(SimpleThrift::MapType.new(SimpleThrift::STRING, SimpleThrift::I32), { "cat" => 5 }).should == "\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05"
    end

    it "set" do
      SimpleThrift.pack_value(SimpleThrift::SetType.new(SimpleThrift::I32), [ 4 ]).should == "\x08\x00\x00\x00\x01\x00\x00\x00\x04"
    end

    it "struct" do
      SimpleThrift.pack_value(SimpleThrift::StructType.new(struct), struct.new("Commie")).should == "\x0b\x00\x01\x00\x00\x00\x06Commie\x00"
    end

    it "request" do
      SimpleThrift.pack_request("getHeight", arg_struct.new, 23).should == "\x80\x01\x00\x01\x00\x00\x00\x09getHeight\x00\x00\x00\x17\x00"
    end
  end

  context "decodes" do
    it "boolean" do
      SimpleThrift.read_value(StringIO.new("\x01"), SimpleThrift::BOOL).should == true
      SimpleThrift.read_value(StringIO.new("\x00"), SimpleThrift::BOOL).should == false
    end

    it "byte" do
      SimpleThrift.read_value(StringIO.new("\xc7"), SimpleThrift::BYTE).should == -57
    end

    it "i16" do
      SimpleThrift.read_value(StringIO.new("\x00\x96"), SimpleThrift::I16).should == 150
    end

    it "i32" do
      SimpleThrift.read_value(StringIO.new("\x00\x96\xb4\x3f"), SimpleThrift::I32).should == 9876543
    end

    it "i64" do
      SimpleThrift.read_value(StringIO.new("\x00\x00\x00\x1c\xbb\xf3\x09\x04"), SimpleThrift::I64).should == 123412351236
    end

    it "double" do
      SimpleThrift.read_value(StringIO.new("\x40\x23\x00\x00\x00\x00\x00\x00"), SimpleThrift::DOUBLE).should == 9.5
    end

    it "string" do
      SimpleThrift.read_value(StringIO.new("\x00\x00\x00\x05hello"), SimpleThrift::STRING).should == "hello"
    end

    it "list" do
      SimpleThrift.read_value(StringIO.new("\x08\x00\x00\x00\x03\x00\x00\x00\x17\x00\x00\x00\x16\x00\x00\x00\x15"), SimpleThrift::ListType.new(SimpleThrift::I32)).should == [ 23, 22, 21 ]
    end

    it "map" do
      SimpleThrift.read_value(StringIO.new("\x0b\x08\x00\x00\x00\x01\x00\x00\x00\x03cat\x00\x00\x00\x05"), SimpleThrift::MapType.new(SimpleThrift::STRING, SimpleThrift::I32)).should == { "cat" => 5 }
    end

    it "set" do
      SimpleThrift.read_value(StringIO.new("\x08\x00\x00\x00\x01\x00\x00\x00\x04"), SimpleThrift::ListType.new(SimpleThrift::I32)).should == [ 4 ]
    end

    it "struct" do
      SimpleThrift.read_value(StringIO.new("\x0b\x00\x01\x00\x00\x00\x06Commie\x00"), SimpleThrift::StructType.new(struct)).should == struct.new("Commie")
    end

    it "response" do
      SimpleThrift.read_response(StringIO.new("\x80\x01\x00\x02\x00\x00\x00\x09getHeight\x00\x00\x00\xff\x08\x00\x00\x00\x00\x00\x01\x00"), rv_struct).should == [ "getHeight", 255, 1 ]
    end
  end
end
