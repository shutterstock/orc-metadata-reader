#include "core.h"

#ifdef HAS_ZLIB
#  include "zlib.h"
#endif

#ifdef HAS_SNAPPY
#  include "snappy-c.h"
#endif

#ifdef HAS_LZO
#  include "lzo.h"
#endif

#ifdef HAS_LZ4
#  include "lz4.h"
#endif


typedef struct orc__inflate_result_t {
  size_t size;
  uint8_t *output;
  uint64_t max_output;
} orc__inflate_result_t;


#ifdef HAS_SNAPPY
  int orc__inflate__snappy(uint8_t *compressed_input, size_t size, orc__inflate_result_t *result) {
    
    if (snappy_uncompressed_length((char *) compressed_input, size, &result->size) != 0) { 
      return ORC__DECOMPRESS_ERR; 
    }
  
    if (snappy_uncompress((char *) compressed_input, size, (char *) result->output, &result->size) != 0) { 
      return ORC__DECOMPRESS_ERR; 
    }
  
    return ORC__DECOMPRESS_OK;
  }
#endif

#ifdef HAS_ZLIB
  int orc__inflate__zlib(uint8_t *compressed_input, size_t size, orc__inflate_result_t *result) {
    z_stream strm;
    strm.total_in = size;
    strm.total_out = result->size;
    strm.avail_in = size;
    strm.avail_out = result->max_output;
    strm.next_in = compressed_input;
    strm.next_out = result->output;
  
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
  
    if (inflateInit2(&strm, -15) != Z_OK) {
      inflateEnd(&strm);
      return ORC__DECOMPRESS_ERR;
    }
  
    if (inflate(&strm, Z_FINISH) != Z_STREAM_END) {
      inflateEnd(&strm);
      return ORC__DECOMPRESS_ERR;
    }
    result->size = strm.total_out;
    inflateEnd(&strm);
    return ORC__DECOMPRESS_OK;
  }
#endif

#ifdef HAS_LZO
  int orc__inflate__lzo(uint8_t *compressed_input, size_t size, orc__inflate_result_t *result) {
    if (lzo1x_decompress(compressed_input, size, result->output, &result->size, NULL) != LZO_E_OK) { 
      return ORC__DECOMPRESS_ERR; 
    }
    return ORC__DECOMPRESS_OK;
  }
#endif

#ifdef HAS_LZ4
  int orc__inflate__lz4(uint8_t *compressed_input, size_t size, orc__inflate_result_t *result) {
    result->size = LZ4_decompress_safe((char *) compressed_input, (char *) result->output, size, result->max_output);
    if (result->size <= 0) { 
      return ORC__DECOMPRESS_ERR; 
    }
    return ORC__DECOMPRESS_OK;
  }
#endif
