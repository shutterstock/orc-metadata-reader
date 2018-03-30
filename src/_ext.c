#include <Python.h>
#include "reader.h"

#define Py_MEMCHECK(val) if (val == NULL) return PyErr_NoMemory();
#define PyString_CONCAT(string, newpart) PyString_Concat(string, newpart); Py_DECREF(newpart);

static PyObject *ORCReadException;
static PyObject *read_metadata(PyObject *self, PyObject *args, PyObject *kwargs);

void orc__build_schema(PyObject **output, Orc__Proto__Type **types, Orc__Proto__Type *type);

static PyObject *read_metadata(PyObject *self, PyObject *args, PyObject *kwargs) {

  const char *input_path;
  int enable_schema = 0;
  int enable_file_stats = 0;
  int enable_stripe_stats = 0;
  int enable_stripes = 0;
  static char *kwlist[] = {"input_path", "schema", "file_stats", "stripe_stats", "stripes", NULL};

  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|iiii", kwlist, &input_path, &enable_schema,
                                   &enable_file_stats, &enable_stripe_stats, &enable_stripes)) {
    PyErr_BadArgument();
    return NULL;
  }

  /* Initialize reader */
  orc__reader_t *reader;
  if ((reader = orc__reader__init(input_path, enable_stripe_stats, enable_stripes)) == NULL) {
    PyErr_SetFromErrno(PyExc_OSError);
    return NULL;
  }

  /* Decode ORC file */
  int decode_status;
  if ((decode_status = orc__reader__decode(reader)) != ORC__OK) {
    orc__reader__free(reader);

    if (decode_status == ORC__ENOMEM) {
      return PyErr_NoMemory();
    }

    if (decode_status == ORC__DECOMPRESS_ERR) {
      PyErr_SetString(ORCReadException, "Could not decompress file.");
      return NULL;
    }

    if (decode_status == ORC__NOSTREAM) {
      PyErr_SetString(ORCReadException, "Could not read partial file.");
      return NULL;
    }
  }
  int i, j;
  PyObject *value, *ret = PyDict_New();
  Py_MEMCHECK(ret);


  /* Encode metadata as PyObject */

  value = Py_BuildValue("K", reader->footer->numberofrows);
  Py_MEMCHECK(value);

  PyDict_SetItemString(ret, "rows", value);
  Py_DECREF(value);
  
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__NONE) {
    value = PyString_FromString("NONE"); 
  }
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__ZLIB) {
    value = PyString_FromString("ZLIB");
  }
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__SNAPPY) {
    value = PyString_FromString("SNAPPY");
  }
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__LZO) {
    value = PyString_FromString("LZO");
  }
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__LZ4) {
    value = PyString_FromString("LZ4");
  }
  if (reader->post_script->compression == ORC__COMPRESSION_KIND__ZSTD) {
    value = PyString_FromString("ZSTD");
  }
  Py_MEMCHECK(value);
  PyDict_SetItemString(ret, "compression", value);
  Py_DECREF(value);

  if (reader->post_script->writerversion == 0) {
    value = PyString_FromFormat("%i.%i with original", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 1) {
    value = PyString_FromFormat("%i.%i with HIVE-8732", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 2) {
    value = PyString_FromFormat("%i.%i with HIVE-4243", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 3) {
    value = PyString_FromFormat("%i.%i with HIVE-12055", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 4) {
    value = PyString_FromFormat("%i.%i with HIVE-13083", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 5) {
    value = PyString_FromFormat("%i.%i with ORC-101", reader->post_script->version[0], reader->post_script->version[1]);
  }
  if (reader->post_script->writerversion == 6) {
    value = PyString_FromFormat("%i.%i with ORC-135", reader->post_script->version[0], reader->post_script->version[1]);
  }
  Py_MEMCHECK(value);
  PyDict_SetItemString(ret, "version", value); 
  Py_DECREF(value);

  value = Py_BuildValue("K", reader->post_script->compressionblocksize);
  Py_MEMCHECK(value);
  PyDict_SetItemString(ret, "compression_size", value); 
  Py_DECREF(value);

 
  /* Build schema */
  if (enable_schema) {
    PyObject *output = PyString_FromString("");
    Py_MEMCHECK(output);

    orc__build_schema(&output, reader->footer->types, reader->footer->types[0]);

    PyDict_SetItemString(ret, "schema", output);
    Py_DECREF(output);
  }

  PyObject *ptr, *stripe_stats, *col_stats, *stripe_stat_section;


  /* Build Stripe Statistics */
  if (enable_stripe_stats) {
    stripe_stats = PyList_New(reader->metadata->n_stripestats);
    Py_MEMCHECK(stripe_stats);
    for (i=0; i < reader->metadata->n_stripestats; ++i) {

      stripe_stat_section = PyDict_New();
      Py_MEMCHECK(stripe_stat_section);

      col_stats = PyList_New(reader->metadata->stripestats[i]->n_colstats);
      Py_MEMCHECK(col_stats);

      for (j=0; j < reader->metadata->stripestats[i]->n_colstats; ++j) {
        ptr = PyDict_New();
        Py_MEMCHECK(ptr);

        value = Py_BuildValue("i", j);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "column", value);
        Py_DECREF(value);

        value = PyBool_FromLong(reader->metadata->stripestats[i]->colstats[j]->hasnull);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "has null", value);
        Py_DECREF(value);

        value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->numberofvalues);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "count", value);
        Py_DECREF(value);
 
        if (reader->metadata->stripestats[i]->colstats[j]->intstatistics != NULL) {
          if (reader->metadata->stripestats[i]->colstats[j]->intstatistics->has_minimum) { 
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->intstatistics->minimum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "min", value);
            Py_DECREF(value);
          }
          if (reader->metadata->stripestats[i]->colstats[j]->intstatistics->has_maximum) {
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->intstatistics->maximum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "max", value);
            Py_DECREF(value);
          }
          if (reader->metadata->stripestats[i]->colstats[j]->intstatistics->has_sum) {
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->intstatistics->sum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "sum", value);
            Py_DECREF(value);
          }
        }
    
        if (reader->metadata->stripestats[i]->colstats[j]->doublestatistics != NULL) {
          if (reader->metadata->stripestats[i]->colstats[j]->doublestatistics->has_minimum) {
            value = Py_BuildValue("d", reader->metadata->stripestats[i]->colstats[j]->doublestatistics->minimum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "min", value);
            Py_DECREF(value);
          }
          if (reader->metadata->stripestats[i]->colstats[j]->doublestatistics->has_maximum) {
            value = Py_BuildValue("d", reader->metadata->stripestats[i]->colstats[j]->doublestatistics->maximum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "max", value);
            Py_DECREF(value);
          }
          if (reader->metadata->stripestats[i]->colstats[j]->doublestatistics->has_sum) {
            value = Py_BuildValue("d", reader->metadata->stripestats[i]->colstats[j]->doublestatistics->sum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "sum", value);
            Py_DECREF(value);
          }
        }
    
        if (reader->metadata->stripestats[i]->colstats[j]->stringstatistics != NULL) {
          value = Py_BuildValue("s", reader->metadata->stripestats[i]->colstats[j]->stringstatistics->minimum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "min", value);
          Py_DECREF(value);
    
          value = Py_BuildValue("s", reader->metadata->stripestats[i]->colstats[j]->stringstatistics->maximum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "max", value);
          Py_DECREF(value);
    
          if (reader->metadata->stripestats[i]->colstats[j]->stringstatistics->has_sum) {
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->stringstatistics->sum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "sum", value);
            Py_DECREF(value);
          }
        }
    
        if (reader->metadata->stripestats[i]->colstats[j]->decimalstatistics != NULL) { 
          value = Py_BuildValue("s", reader->metadata->stripestats[i]->colstats[j]->decimalstatistics->minimum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "min", value);
          Py_DECREF(value);
    
          value = Py_BuildValue("s", reader->metadata->stripestats[i]->colstats[j]->decimalstatistics->maximum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "max", value);
          Py_DECREF(value);
    
          value = Py_BuildValue("s", reader->metadata->stripestats[i]->colstats[j]->decimalstatistics->sum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "sum", value);
          Py_DECREF(value);
        }
    
        if (reader->metadata->stripestats[i]->colstats[j]->datestatistics != NULL) {
          if (reader->metadata->stripestats[i]->colstats[j]->datestatistics->has_minimum) {
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->datestatistics->minimum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "min", value);
            Py_DECREF(value);
          }
          if (reader->metadata->stripestats[i]->colstats[j]->datestatistics->has_maximum) {
            value = Py_BuildValue("i", reader->metadata->stripestats[i]->colstats[j]->datestatistics->maximum);
            Py_MEMCHECK(value);
            PyDict_SetItemString(ptr, "max", value);
            Py_DECREF(value);
          }
        }
        PyList_SetItem(col_stats, j, ptr);
      }

      value = Py_BuildValue("i", i);
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe_stat_section, "stripe", value);
      Py_DECREF(value);

      PyDict_SetItemString(stripe_stat_section, "statistics", col_stats);
      Py_DECREF(col_stats);

      PyList_SetItem(stripe_stats, i, stripe_stat_section);
    }

    PyDict_SetItemString(ret, "Stripe Statistics", stripe_stats);
    Py_DECREF(stripe_stats);
  }

  /* Build File Statistics */
  if (enable_file_stats) {
    col_stats = PyList_New(reader->footer->n_statistics);
    Py_MEMCHECK(col_stats);
    for (i=0; i < reader->footer->n_statistics; ++i) {
      ptr = PyDict_New();
      Py_MEMCHECK(ptr);

      value = Py_BuildValue("i", i);
      Py_MEMCHECK(value);
      PyDict_SetItemString(ptr, "column", value);
      Py_DECREF(value);

      value = PyBool_FromLong(reader->footer->statistics[i]->hasnull);
      Py_MEMCHECK(value);
      PyDict_SetItemString(ptr, "has null", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->statistics[i]->numberofvalues);
      Py_MEMCHECK(value);
      PyDict_SetItemString(ptr, "count", value);
      Py_DECREF(value);

      if (reader->footer->statistics[i]->intstatistics != NULL) {
        if (reader->footer->statistics[i]->intstatistics->has_minimum) { 
          value = Py_BuildValue("i", reader->footer->statistics[i]->intstatistics->minimum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "min", value);
          Py_DECREF(value);
        }
        if (reader->footer->statistics[i]->intstatistics->has_maximum) {
          value = Py_BuildValue("i", reader->footer->statistics[i]->intstatistics->maximum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "max", value);
          Py_DECREF(value);
        }
        if (reader->footer->statistics[i]->intstatistics->has_sum) {
          value = Py_BuildValue("i", reader->footer->statistics[i]->intstatistics->sum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "sum", value);
          Py_DECREF(value);
        }
      }

      if (reader->footer->statistics[i]->doublestatistics != NULL) {
        if (reader->footer->statistics[i]->doublestatistics->has_minimum) {
          value = Py_BuildValue("d", reader->footer->statistics[i]->doublestatistics->minimum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "min", value);
          Py_DECREF(value);
        }
        if (reader->footer->statistics[i]->doublestatistics->has_maximum) {
          value = Py_BuildValue("d", reader->footer->statistics[i]->doublestatistics->maximum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "max", value);
          Py_DECREF(value);
        }
        if (reader->footer->statistics[i]->doublestatistics->has_sum) {
          value = Py_BuildValue("d", reader->footer->statistics[i]->doublestatistics->sum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "sum", value);
          Py_DECREF(value);
        }
      }

      if (reader->footer->statistics[i]->stringstatistics != NULL) {
        value = Py_BuildValue("s", reader->footer->statistics[i]->stringstatistics->minimum);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "min", value);
        Py_DECREF(value);

        value = Py_BuildValue("s", reader->footer->statistics[i]->stringstatistics->maximum);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "max", value);
        Py_DECREF(value);

        if (reader->footer->statistics[i]->stringstatistics->has_sum) {
          value = Py_BuildValue("i", reader->footer->statistics[i]->stringstatistics->sum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "sum", value);
          Py_DECREF(value);
        }
      }

      if (reader->footer->statistics[i]->decimalstatistics != NULL) { 
        value = Py_BuildValue("s", reader->footer->statistics[i]->decimalstatistics->minimum);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "min", value);
        Py_DECREF(value);

        value = Py_BuildValue("s", reader->footer->statistics[i]->decimalstatistics->maximum);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "max", value);
        Py_DECREF(value);

        value = Py_BuildValue("s", reader->footer->statistics[i]->decimalstatistics->sum);
        Py_MEMCHECK(value);
        PyDict_SetItemString(ptr, "sum", value);
        Py_DECREF(value);
      }

      if (reader->footer->statistics[i]->datestatistics != NULL) {
        if (reader->footer->statistics[i]->datestatistics->has_minimum) {
          value = Py_BuildValue("i", reader->footer->statistics[i]->datestatistics->minimum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "min", value);
          Py_DECREF(value);
        }
        if (reader->footer->statistics[i]->datestatistics->has_maximum) {
          value = Py_BuildValue("i", reader->footer->statistics[i]->datestatistics->maximum);
          Py_MEMCHECK(value);
          PyDict_SetItemString(ptr, "max", value);
          Py_DECREF(value);
        }
      }
      PyList_SetItem(col_stats, i, ptr);
    }

    PyDict_SetItemString(ret, "File Statistics", col_stats);
    Py_DECREF(col_stats);
  }

  /* Build Stripe Footers */
  if (enable_stripes) {
    PyObject *stripes, *stripe, *stream, *encoding_col;
    PyObject *stream_list, *encoding_list;

    stripes = PyList_New(reader->footer->n_stripes);
    Py_MEMCHECK(stripes);

    int64_t stream_offset;
    for (i=0; i < reader->footer->n_stripes; ++i) {
      stripe = PyDict_New();
      Py_MEMCHECK(stripe);

      value = Py_BuildValue("i", i);
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "stripe", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->stripes[i]->offset);  
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "offset", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->stripes[i]->datalength);  
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "data", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->stripes[i]->numberofrows);  
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "rows", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->stripes[i]->footerlength);
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "tail", value);
      Py_DECREF(value);

      value = Py_BuildValue("i", reader->footer->stripes[i]->indexlength);
      Py_MEMCHECK(value);
      PyDict_SetItemString(stripe, "index", value);
      Py_DECREF(value);

      stream_offset = reader->footer->stripes[i]->offset;
      stream_list = PyList_New(reader->stripe_footers[i]->n_streams);
      Py_MEMCHECK(stream_list);
      for (j=0; j < reader->stripe_footers[i]->n_streams; ++j) { 
        stream = PyDict_New();
        Py_MEMCHECK(stream);

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__PRESENT) {
          value = PyString_FromString("PRESENT");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__DATA) {
          value = PyString_FromString("DATA");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__LENGTH) {
          value = PyString_FromString("LENGTH");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__DICTIONARY_DATA) {
          value = PyString_FromString("DICTIONARY_DATA");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__DICTIONARY_COUNT) {
          value = PyString_FromString("DICTIONARY_COUNT");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__SECONDARY) {
          value = PyString_FromString("SECONDARY");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__ROW_INDEX) {
          value = PyString_FromString("ROW_INDEX");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__BLOOM_FILTER) {
          value = PyString_FromString("BLOOM_FILTER");
        }

        if (reader->stripe_footers[i]->streams[j]->kind == ORC__STREAM_KIND__BLOOM_FILTER_UTF8) {
          value = PyString_FromString("BLOOM_FILTER_UTF8");
        }
        Py_MEMCHECK(value);
        PyDict_SetItemString(stream, "section", value);
        Py_DECREF(value);

        value = Py_BuildValue("i", reader->stripe_footers[i]->streams[j]->column);
        Py_MEMCHECK(value);
        PyDict_SetItemString(stream, "column", value);
        Py_DECREF(value);

        value = Py_BuildValue("i", stream_offset);
        Py_MEMCHECK(value);
        PyDict_SetItemString(stream, "start", value);
        Py_DECREF(value);

        value = Py_BuildValue("i", reader->stripe_footers[i]->streams[j]->length);
        Py_MEMCHECK(value);
        PyDict_SetItemString(stream, "length", value);
        Py_DECREF(value);

        PyList_SetItem(stream_list, j, stream);
        
        stream_offset += reader->stripe_footers[i]->streams[j]->length;
      }

      encoding_list = PyList_New(reader->stripe_footers[i]->n_columns); 
      Py_MEMCHECK(encoding_list);
      for (j=0; j < reader->stripe_footers[i]->n_columns; ++j) {
        encoding_col = PyDict_New();
        Py_MEMCHECK(encoding_col);

        if (reader->stripe_footers[i]->columns[j]->kind == ORC__COLUMN_ENCODING_KIND__DIRECT) {
          value = PyString_FromString("DIRECT");
        }
        if (reader->stripe_footers[i]->columns[j]->kind == ORC__COLUMN_ENCODING_KIND__DICTIONARY) {
          value = PyString_FromFormat("DICTIONARY[%i]", reader->stripe_footers[i]->columns[j]->dictionarysize);
        }
        if (reader->stripe_footers[i]->columns[j]->kind == ORC__COLUMN_ENCODING_KIND__DIRECT_V2) {
          value = PyString_FromString("DIRECT_V2");
        }
        if (reader->stripe_footers[i]->columns[j]->kind == ORC__COLUMN_ENCODING_KIND__DICTIONARY_V2) {
          value = PyString_FromFormat("DICTIONARY_V2[%i]", reader->stripe_footers[i]->columns[j]->dictionarysize);
        }
        Py_MEMCHECK(value);
        PyDict_SetItemString(encoding_col, "encoding", value);
        Py_DECREF(value);

        value = Py_BuildValue("i", j);
        Py_MEMCHECK(value);
        PyDict_SetItemString(encoding_col, "column", value);
        Py_DECREF(value);

        PyList_SetItem(encoding_list, j, encoding_col);
      }

      PyDict_SetItemString(stripe, "Streams", stream_list);
      Py_DECREF(stream_list);

      PyDict_SetItemString(stripe, "Encodings", encoding_list);
      Py_DECREF(encoding_list);

      PyList_SetItem(stripes, i, stripe);
    }

    PyDict_SetItemString(ret, "Stripes", stripes);
    Py_DECREF(stripes);
  }

  orc__reader__free(reader);
  return ret;
}

void orc__build_schema(PyObject **output, Orc__Proto__Type **types, Orc__Proto__Type *type) {
  PyObject *val;

  if (type->kind == ORC__TYPE_KIND__BOOLEAN) {
    val = PyString_FromString("boolean");
  }
  if (type->kind == ORC__TYPE_KIND__BYTE) {
    val = PyString_FromString("byte");
  }
  if (type->kind == ORC__TYPE_KIND__SHORT) {
    val = PyString_FromString("tinyint");
  }
  if (type->kind == ORC__TYPE_KIND__INT) {
    val = PyString_FromString("int");
  }
  if (type->kind == ORC__TYPE_KIND__LONG) {
    val = PyString_FromString("bigint");
  }
  if (type->kind == ORC__TYPE_KIND__FLOAT) {
    val = PyString_FromString("float");
  }
  if (type->kind == ORC__TYPE_KIND__DOUBLE) {
    val = PyString_FromString("double");
  }
  if (type->kind == ORC__TYPE_KIND__STRING) {
    val = PyString_FromString("string");
  }
  if (type->kind == ORC__TYPE_KIND__BINARY) {
    val = PyString_FromString("binary");
  }
  if (type->kind == ORC__TYPE_KIND__TIMESTAMP) {
    val = PyString_FromString("timestamp");
  }
  if (type->kind == ORC__TYPE_KIND__LIST) {
    val = PyString_FromString("array<");
  }
  if (type->kind == ORC__TYPE_KIND__MAP) {
    val = PyString_FromString("map<");
  }
  if (type->kind == ORC__TYPE_KIND__STRUCT) {
    val = PyString_FromString("struct<");
  }
  if (type->kind == ORC__TYPE_KIND__UNION) {
    val = PyString_FromString("union<");
  }
  if (type->kind == ORC__TYPE_KIND__DECIMAL) {
    val = PyString_FromString("decimal");
  }
  if (type->kind == ORC__TYPE_KIND__DATE) {
    val = PyString_FromString("date");
  }
  if (type->kind == ORC__TYPE_KIND__VARCHAR) {
    val = PyString_FromString("varchar");
  }
  if (type->kind == ORC__TYPE_KIND__CHAR) {
    val = PyString_FromString("char");
  }

  PyString_CONCAT(output, val);
  
  int i;
  for (i=0;i<type->n_subtypes; ++i) {
    if (type->n_fieldnames) {
      PyString_CONCAT(output, PyString_FromString(type->fieldnames[i]));
      PyString_CONCAT(output, PyString_FromString(":"));
    }

    orc__build_schema(output, types, types[type->subtypes[i]]);

    if (i<type->n_subtypes-1) { 
      PyString_CONCAT(output, PyString_FromString(","));
    }
  }
  if (type->kind == 10 || type->kind == 11 || type->kind == 12 || type->kind == 13) { 
    PyString_CONCAT(output, PyString_FromString(">"));
  }
}

static char module_docstring[] = "This module provides an interface for reading ORC files in C.";
static char func_docstring[] = "Read ORC file metadata.";

static PyMethodDef module_methods[] = {
      {"read_metadata", (PyCFunction) read_metadata, METH_VARARGS|METH_KEYWORDS, func_docstring},
      {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC init_orc_metadata(void) {
  PyObject *mod;
  mod = Py_InitModule3("_orc_metadata", module_methods, module_docstring);
  if (mod == NULL)
    return;
  
  ORCReadException = PyErr_NewException("_orc_metadata.ORCReadException", NULL, NULL);
  Py_INCREF(ORCReadException);
  PyModule_AddObject(mod, "ORCReadException", ORCReadException);
}
