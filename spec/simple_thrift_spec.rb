
require "lib/simple_thrift"

describe "SimpleThrift" do
  before do
  end

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
      struct = SimpleThrift.make_struct("Example", SimpleThrift::Field.new(:name, SimpleThrift::STRING, 1))
      SimpleThrift.pack_value(SimpleThrift::StructType.new(struct), struct.new("Commie")).should == "\x0b\x00\x01\x00\x00\x00\x06Commie\x00"
    end

    it "request" do
      arg_struct = SimpleThrift.make_struct("Args")
      SimpleThrift.pack_request("getHeight", arg_struct.new, 23).should == "\x80\x01\x00\x01\x00\x00\x00\x09getHeight\x00\x00\x00\x17\x00"
    end
  end
end


#
#     "decode" in {
#       "boolean" in {
#         decoder(makeBuffer("0100"), Codec.readBoolean { x => decoder.write(x.toString); End }) mustEqual List("true", "false")
#       }
#
#       "byte" in {
#         decoder(makeBuffer("c7"), Codec.readByte { x => decoder.write(x.toString); End }) mustEqual List("-57")
#       }
#
#       "i16" in {
#         decoder(makeBuffer("0096"), Codec.readI16 { x => decoder.write(x.toString); End }) mustEqual List("150")
#       }
#
#       "i32" in {
#         decoder(makeBuffer("0096b43f"), Codec.readI32 { x => decoder.write(x.toString); End }) mustEqual List("9876543")
#       }
#
#       "i64" in {
#         decoder(makeBuffer("0000001cbbf30904"), Codec.readI64 { x => decoder.write(x.toString); End }) mustEqual List("123412351236")
#       }
#
#       "double" in {
#         decoder(makeBuffer("4023000000000000"), Codec.readDouble { x => decoder.write(x.toString); End }) mustEqual List("9.5")
#       }
#
#       "string" in {
#         decoder(makeBuffer("0000000568656c6c6f"), Codec.readString { x => decoder.write(x.toString); End }) mustEqual List("hello")
#       }
#
#       "binary" in {
#         decoder(makeBuffer("00000003636174"), Codec.readBinary { x => decoder.write(new String(x)); End }) mustEqual List("cat")
#       }
#
#       "list" in {
#         decoder(makeBuffer("08000000030096b43f0096b43f0096b43f"), Codec.readList[Int](Type.I32) { f => Codec.readI32 { item => f(item) } } { x => decoder.write(x.toString); End }) mustEqual List("List(9876543, 9876543, 9876543)")
#       }
#
#       "map" in {
#         decoder(makeBuffer("0b0800000001000000036361740096b43f"), Codec.readMap[String, Int](Type.STRING, Type.I32) { f => Codec.readString { item => f(item) } } { f => Codec.readI32 { item => f(item) } } { x => decoder.write(x.toString); End }) mustEqual List("Map(cat -> 9876543)")
#       }
#
#       "set" in {
#         decoder(makeBuffer("0800000001000000ff"), Codec.readSet[Int](Type.I32) { f => Codec.readI32 { item => f(item) } } { x => decoder.write(x.toString); End }) mustEqual List("Set(255)")
#       }
#
#       "field header" in {
#         decoder(makeBuffer("0c002c"), Codec.readFieldHeader { x => decoder.write(x.toString); End }) mustEqual List("FieldHeader(12,44)")
#       }
#
#       "request header" in {
#         decoder(makeBuffer("800100010000000967657448656967687400000017"), Codec.readRequestHeader { x => decoder.write(x.toString); End }) mustEqual List("RequestHeader(1,getHeight,23)")
#       }
#     }
#
#     "skip" in {
#       decoder = new TestDecoder
#       decoder(makeBuffer("0123"), Codec.skip(Type.BOOL) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("c723"), Codec.skip(Type.BYTE) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("009623"), Codec.skip(Type.I16) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0096b43f23"), Codec.skip(Type.I32) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0000001cbbf3090423"), Codec.skip(Type.I64) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("402300000000000023"), Codec.skip(Type.DOUBLE) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0000000568656c6c6f23"), Codec.skip(Type.STRING) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0301ff1f0023"), Codec.skip(Type.STRUCT) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("08000000010096b43f23"), Codec.skip(Type.LIST) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0b0800000001000000036361740096b43f23"), Codec.skip(Type.MAP) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#       decoder = new TestDecoder
#       decoder(makeBuffer("0800000001000000ff23"), Codec.skip(Type.SET) { Codec.readByte { x => decoder.write(x.toString); End } }) mustEqual List("35")
#     }
#   }
# }
