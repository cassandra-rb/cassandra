#include <ruby.h>
#include <arpa/inet.h>

VALUE parts_ivar_id, types_ivar_id;

VALUE rb_cassandra_composite_fast_unpack(VALUE self, VALUE packed_string_value) {
  int index = 0;
  int message_length = RSTRING_LEN(packed_string_value);
  char *packed_string = (char *)RSTRING_PTR(packed_string_value);

  VALUE parts = rb_ary_new();
  while (index < message_length) {
    uint16_t length = ntohs(((uint16_t *)(packed_string+index))[0]);
    VALUE part = rb_str_new(packed_string+index+2, length);
    rb_ary_push(parts, part);
    index += length + 3;
  }

  rb_ivar_set(self, parts_ivar_id, parts);
  return Qnil;
}

VALUE rb_cassandra_dynamic_composite_fast_unpack(VALUE self, VALUE packed_string_value) {
  int index = 0;
  int message_length = RSTRING_LEN(packed_string_value);
  char *packed_string = (char *)RSTRING_PTR(packed_string_value);
  uint16_t length;

  VALUE parts = rb_ary_new();
  VALUE types = rb_ary_new();
  while (index < message_length) {
    if (packed_string[index] & 0x80) {
      VALUE type = rb_str_new(packed_string + index + 1, 1);
      rb_ary_push(types, type);
      index += 2;
    } else {
      length = ntohs(((uint16_t *)(packed_string+index))[0]);
      VALUE type = rb_str_new(packed_string + index + 2, length);
      rb_ary_push(types, type);
      index += 2 + length;
    }

    length = ntohs(((uint16_t *)(packed_string+index))[0]);
    VALUE part = rb_str_new(packed_string + index + 2, length);
    rb_ary_push(parts, part);
    index += length + 3;
  }

  rb_ivar_set(self, parts_ivar_id, parts);
  rb_ivar_set(self, types_ivar_id, types);

  return Qnil;
}

void Init_cassandra_native(void) {
  VALUE cassandra_module = rb_const_get(rb_cObject, rb_intern("Cassandra"));
  VALUE cassandra_composite_class = rb_define_class_under(cassandra_module, "Composite", rb_cObject);
  rb_define_method(cassandra_composite_class, "fast_unpack", rb_cassandra_composite_fast_unpack, 1);

  VALUE dynamic_composite = rb_const_get(cassandra_module, rb_intern("DynamicComposite"));
  rb_define_method(dynamic_composite, "fast_unpack", rb_cassandra_dynamic_composite_fast_unpack, 1);

  parts_ivar_id = rb_intern("@parts");
  types_ivar_id = rb_intern("@types");
}
