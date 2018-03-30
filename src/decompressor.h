#include "core.h"
#include "inflate.h"
#include "buffer.h"


typedef struct orc__block_t {
  size_t size;
  uint8_t is_compressed;
} orc__block_t;


typedef struct orc__decompressor_t {
  orc__buffer_t *input;
  orc__buffer_t *output;

  uint8_t compression_kind;
  uint64_t size;
  uint64_t output_buffer_size;

  orc__block_t *current_block;
} orc__decompressor_t;


orc__decompressor_t *orc__decompressor_init(uint8_t compression_kind, uint64_t compression_block_size, 
                                            uint8_t *compressed_stream, uint64_t size) {
  orc__decompressor_t *decomp;
  if ((decomp = malloc(sizeof(orc__decompressor_t))) == NULL) { 
    return NULL; 
  }

  decomp->compression_kind = compression_kind;
  decomp->size = size;
  decomp->input = orc__buffer__init_from_stream(compressed_stream);

  if (size > compression_block_size) {
    decomp->output_buffer_size = size;
  } else {
    decomp->output_buffer_size = compression_block_size;
  }

  if ((decomp->current_block = malloc(sizeof(orc__block_t))) == NULL) { 
    return NULL; 
  }

  if ((decomp->output = orc__buffer__init(decomp->output_buffer_size)) == NULL) { 
    return NULL; 
  }

  return decomp;
}

void orc__decompressor__decode_header(orc__decompressor_t *decomp) {
  uint8_t *block = decomp->input->ptr;
  uint32_t header = block[0];
  header |= block[1] << 8;
  header |= block[2] << 16;
  decomp->current_block->is_compressed = header & 1;
  decomp->current_block->size = header >> 1;
  orc__buffer__forward(decomp->input, ORC__BLOCK_HEADER_SIZE);
}

uint8_t orc__decompressor__decode(orc__decompressor_t *decomp) {
  if (decomp->compression_kind == ORC__COMPRESSION_KIND__NONE) {
    orc__buffer__append(decomp->output, decomp->input->ptr, decomp->size);
    return ORC__DECOMPRESS_OK;
  }

  orc__inflate_result_t *result = malloc(sizeof(orc__inflate_result_t));
  result->max_output = decomp->output_buffer_size;

  int compression_found = 0;
  while (decomp->input->size < decomp->size) {
    orc__decompressor__decode_header(decomp);

    if (decomp->current_block->is_compressed == 1) { 
      orc__buffer__append(decomp->output, decomp->input->ptr, decomp->current_block->size); 
    }
    else {
      result->output = decomp->output->ptr;
      result->size = decomp->current_block->size;

#ifdef HAS_SNAPPY
      if (decomp->compression_kind == ORC__COMPRESSION_KIND__SNAPPY) { 
        compression_found = 1;
        orc__inflate__snappy(decomp->input->ptr, decomp->current_block->size, result); 
      }
#endif

#ifdef HAS_ZLIB
      if (decomp->compression_kind == ORC__COMPRESSION_KIND__ZLIB) { 
        compression_found = 1;
        orc__inflate__zlib(decomp->input->ptr, decomp->current_block->size, result); 
      }
#endif

#ifdef HAS_LZO
      if (decomp->compression_kind == ORC__COMPRESSION_KIND__LZO) { 
        compression_found = 1;
        orc__inflate__lzo(decomp->input->ptr, decomp->current_block->size, result); 
      }
#endif

#ifdef HAS_LZ4
      if (decomp->compression_kind == ORC__COMPRESSION_KIND__LZ4) { 
        compression_found = 1;
        orc__inflate__lz4(decomp->input->ptr, decomp->current_block->size, result); 
      }
#endif

      if (compression_found == 0) { 
        return ORC__DECOMPRESS_ERR; 
      }

      orc__buffer__forward(decomp->output, result->size);
    }
    orc__buffer__forward(decomp->input, decomp->current_block->size);
  }
  free(result);
  return ORC__DECOMPRESS_OK;
}

void orc__decompressor__free(orc__decompressor_t *decomp) {
  free(decomp->current_block);

  orc__buffer__free(decomp->input);
  orc__buffer__free(decomp->output);

  free(decomp);
}
