#pragma once


typedef struct orc__buffer_t {
  uint8_t *head;
  uint8_t *ptr;
  size_t size;
  uint8_t wraps_stream;
} orc__buffer_t;


orc__buffer_t * orc__buffer__init(size_t size) {
  orc__buffer_t *buf; 

  if ((buf = malloc(sizeof(orc__buffer_t))) == NULL) { 
    return NULL; 
  }

  if ((buf->head = malloc(size)) == NULL) { 
    return NULL; 
  }

  int i;
  for (i=0; i<size; i++) {
    buf->head[i] = 0;
  }

  buf->ptr = buf->head;
  buf->size = 0;
  buf->wraps_stream = 0;
  return buf;
}

orc__buffer_t *orc__buffer__init_from_stream(uint8_t *stream) {
  orc__buffer_t *buf; 

  if ((buf = malloc(sizeof(orc__buffer_t))) == NULL) { 
    return NULL; 
  }

  buf->head = stream;
  buf->ptr = stream;
  buf->size = 0;
  buf->wraps_stream = 1;
  return buf;
}

void orc__buffer__forward(orc__buffer_t *buf, size_t size) {
  buf->ptr += size;
  buf->size += size;
}

void orc__buffer__rewind(orc__buffer_t *buf, size_t size) {
  buf->ptr -= size;
  buf->size -= size;
}

void orc__buffer__rewind_shift(orc__buffer_t *buf, size_t size) {
  orc__buffer__rewind(buf, size);
  buf->head = buf->ptr;
  buf->size = 0;
  orc__buffer__forward(buf, size);
}

void orc__buffer__append(orc__buffer_t *buf, uint8_t *data, size_t len) {
  int i;
  for (i=0; i<len; ++i) {
    buf->ptr[i] = data[i];
  }
  buf->size += len;
}

void orc__buffer__free(orc__buffer_t *buf) {
  if (buf->wraps_stream == 0) { 
    free(buf->head); 
  }
  free(buf);
}
