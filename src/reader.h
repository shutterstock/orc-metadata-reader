#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include "core.h"
#include "orc.pb-c.h"
#include "decompressor.h"
#include "buffer.h"


typedef struct orc__reader_t {
  const char *input_path;
  size_t size;
  uint8_t *data;
  int enable_stripes;
  int enable_stripe_stats;

  int post_script_decoded;
  int footer_decoded;
  int metadata_decoded;
  int stripes_decoded;

  Orc__Proto__PostScript *post_script; 
  Orc__Proto__Footer *footer;
  Orc__Proto__Metadata *metadata;
  Orc__Proto__StripeFooter **stripe_footers;
} orc__reader_t;

int orc__reader__file_to_buffer(orc__reader_t *reader);


orc__reader_t *orc__reader__init(const char *input_path, int enable_stripe_stats, int enable_stripes) {
  orc__reader_t *reader = malloc(sizeof(orc__reader_t));
  reader->enable_stripe_stats = enable_stripe_stats;
  reader->enable_stripes = enable_stripes;
  reader->post_script_decoded = 0;
  reader->footer_decoded = 0;
  reader->metadata_decoded = 0;
  reader->stripes_decoded = 0;
  reader->input_path = input_path;
  orc__reader__file_to_buffer(reader);
  return reader;
}

int orc__reader__decode(orc__reader_t *reader) {
  orc__buffer_t *post_script_buffer = orc__buffer__init_from_stream((uint8_t *) reader->data);

  /* Post script length is the last byte of the file */
  orc__buffer__forward(post_script_buffer, reader->size-1);
  uint64_t post_script_length = *(uint64_t *) post_script_buffer->ptr & 0xff;
  if (post_script_length > reader->size) {
    return ORC__NOSTREAM;
  }

  /* Set the buffer to the beginning of the post script */
  orc__buffer__rewind_shift(post_script_buffer, post_script_length);

  if ((reader->post_script = orc__proto__post_script__unpack(NULL, 
                                                             post_script_buffer->size, 
                                                             &post_script_buffer->head[0])) == NULL) {
    return ORC__NODECODE;
  }
  reader->post_script_decoded = 1;
   
  orc__buffer__free(post_script_buffer);

  uint64_t footer_offset = 1+post_script_length+reader->post_script->footerlength; 
  if (footer_offset > reader->size) {
    return ORC__NOSTREAM;
  }

  uint8_t *compressed_footer = (uint8_t *) reader->data+(reader->size-footer_offset);

  /* Decode footer section */
  orc__decompressor_t *decompressor;
  if ((decompressor = orc__decompressor_init(reader->post_script->compression,
                                             reader->post_script->compressionblocksize, 
                                             compressed_footer, 
                                             reader->post_script->footerlength)) == NULL) { 
    return ORC__ENOMEM; 
  }

  int status;
  if ((status = orc__decompressor__decode(decompressor)) != ORC__OK) {
    orc__decompressor__free(decompressor);
    return status;
  }

  if ((reader->footer = orc__proto__footer__unpack(NULL, 
                                                   decompressor->output->size, 
                                                   &decompressor->output->head[0])) == NULL) {
      orc__decompressor__free(decompressor);
      return ORC__NODECODE;
  }
  reader->footer_decoded = 1;

  orc__decompressor__free(decompressor);

  /* Decode metadata section */
  if (reader->enable_stripe_stats) {
    uint64_t metadata_offset = footer_offset+reader->post_script->metadatalength;
    if (metadata_offset > reader->size) {
      return ORC__NOSTREAM;
    }

    uint8_t *compressed_metadata  = (uint8_t *) reader->data+(reader->size-metadata_offset); 
    if ((decompressor = orc__decompressor_init(reader->post_script->compression,
                                               reader->post_script->compressionblocksize, 
                                               compressed_metadata, 
                                               reader->post_script->metadatalength)) == NULL) { 
      return ORC__ENOMEM; 
    }

    if ((status = orc__decompressor__decode(decompressor)) != ORC__OK) {
      orc__decompressor__free(decompressor);
      return status;
    }

    if ((reader->metadata = orc__proto__metadata__unpack(NULL, 
                                                         decompressor->output->size, 
                                                         &decompressor->output->head[0])) == NULL) {
      orc__decompressor__free(decompressor);
      return ORC__NODECODE;
    }
    reader->metadata_decoded = 1;

    orc__decompressor__free(decompressor);
  }
  
  /* Decode Stripe Footers */
  if (reader->enable_stripes) {
    uint64_t file_length = footer_offset+reader->post_script->metadatalength+reader->footer->contentlength;
    if ((uint64_t) reader->size < file_length) {
      return ORC__NOSTREAM;
    }
 
    int i;
    uint64_t stripe_offset;
    reader->stripe_footers = malloc(sizeof(Orc__Proto__StripeFooter *)*reader->footer->n_stripes);
    for (i=0; i < reader->footer->n_stripes; ++i) {
      stripe_offset = reader->footer->stripes[i]->offset;
      stripe_offset += reader->footer->stripes[i]->indexlength;
      stripe_offset += reader->footer->stripes[i]->datalength;
    
      uint8_t *compressed_stripe  = (uint8_t *) reader->data+(stripe_offset); 
      if ((decompressor = orc__decompressor_init(reader->post_script->compression,
                                                 reader->post_script->compressionblocksize, 
                                                 compressed_stripe, 
                                                 reader->footer->stripes[i]->footerlength)) == NULL) { 
        return ORC__ENOMEM; 
      }
    
      if ((status = orc__decompressor__decode(decompressor)) != ORC__OK) {
        orc__decompressor__free(decompressor);
        return status;
      }

      if ((reader->stripe_footers[i] = orc__proto__stripe_footer__unpack(NULL, 
                                                                         decompressor->output->size, 
                                                                         &decompressor->output->head[0])) == NULL) {
        orc__decompressor__free(decompressor);
        return ORC__NODECODE;
      }

      orc__decompressor__free(decompressor);
      reader->stripes_decoded += 1;
    }
  }

  return status;
}

void orc__reader__free(orc__reader_t *reader) {
  if (reader->enable_stripe_stats && reader->metadata_decoded) {
    orc__proto__metadata__free_unpacked(reader->metadata, NULL);
  }
  if (reader->enable_stripes && reader->stripes_decoded) {
    int i;
    for (i=0; i < reader->stripes_decoded; ++i) {
      if (reader->stripe_footers[i] != NULL) {
        orc__proto__stripe_footer__free_unpacked(reader->stripe_footers[i], NULL);
      }
    }
  }
  if (reader->post_script_decoded) {
    orc__proto__post_script__free_unpacked(reader->post_script, NULL);
  }
  if (reader->footer_decoded) {
    orc__proto__footer__free_unpacked(reader->footer, NULL);
  }

  free((uint8_t *) reader->data);
  free(reader);
}

int orc__reader__file_to_buffer(orc__reader_t *reader) {
  FILE *fp;

  fp = fopen(reader->input_path, "rb");
  if (!fp) { 
    return errno; 
  }
  
  fseek(fp, 0, SEEK_END);
  reader->size = ftell(fp);
  fseek(fp, 0, SEEK_SET);

  reader->data = malloc(sizeof(uint8_t *) * (reader->size));
  if (!reader->data) { 
    fclose(fp); 
    return ORC__ENOMEM;
  }

  size_t ret = fread((uint8_t *) reader->data, reader->size, 1, fp);
  if (ret != reader->size) {
    fclose(fp);
    return ORC__NODECODE;
  }
  fclose(fp);

  return ORC__OK;
}
