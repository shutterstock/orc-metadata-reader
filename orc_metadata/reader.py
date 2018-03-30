import boto3
import tempfile
from _orc_metadata import read_metadata, ORCReadException


class SequenceFileStream(object):
    def __init__(self, file_stream):
        self._file = tempfile.NamedTemporaryFile()
        self._stream = file_stream

    def __enter__(self):
        self._file.write(self._stream.read())
        self._file.seek(0)
        return self._file.name

    def __exit__(self, *exc_args):
        self._file.close()


def _streamed_files(s3_bucket, s3_prefix, fetch_size):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects')
    operation_parameters = {'Bucket': s3_bucket,
                            'Prefix': s3_prefix}

    for result in paginator.paginate(**operation_parameters):
        for obj in result.get('Contents', []):
            if obj.get('Key').endswith('$folder$'):
                continue
            object_kwargs = {'Bucket': s3_bucket, 'Key': obj.get('Key')}
            if fetch_size:
                byte_range = 'bytes={}-'.format(obj.get('Size') - fetch_size)
                object_kwargs['Range'] = byte_range
            resp = s3.get_object(**object_kwargs)
            yield SequenceFileStream(resp['Body'])


def read_metadata_s3(s3_bucket, s3_prefix, fetch_size=None, schema=False,
                     file_stats=False, stripe_stats=False, stripes=False):
    for seq_file in _streamed_files(s3_bucket, s3_prefix, fetch_size):
        with seq_file as temp_file:
            yield read_metadata(temp_file, schema=schema, file_stats=file_stats,
                                stripe_stats=stripe_stats, stripes=stripes)
