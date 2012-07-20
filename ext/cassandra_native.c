#include <ruby.h>
#include <arpa/inet.h>

VALUE parts_ivar_id;

VALUE rb_cassandra_composite_fast_unpack(VALUE self, VALUE packed_string_value) {
  int i = 0;
  int index = 0;
  int message_length = RSTRING_LEN(packed_string_value);
  char *packed_string = (char *)RSTRING_PTR(packed_string_value);

  VALUE parts = rb_ary_new();
  while (index < message_length) {
    uint16_t length = ntohs(((uint16_t *)(packed_string+index))[0]);
    VALUE part = rb_str_new("", length);
    for (i = 0; i < length; i++) {
      ((char *)RSTRING_PTR(part))[i] = packed_string[index+2+i];
    }
    rb_ary_push(parts, part);
    index += length + 3;
  }

  rb_ivar_set(self, parts_ivar_id, parts);

  return Qnil;
}

void Init_cassandra_native(void) {
  VALUE cassandra_module = rb_const_get(rb_cObject, rb_intern("Cassandra"));
  VALUE cassandra_composite_class = rb_define_class_under(cassandra_module, "Composite", rb_cObject);
  rb_define_method(cassandra_composite_class, "fast_unpack", rb_cassandra_composite_fast_unpack, 1);

  parts_ivar_id = rb_intern("@parts");
}
