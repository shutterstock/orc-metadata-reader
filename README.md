ORC Metadata Reader
=====

[![Build Status](https://travis-ci.org/shutterstock/orc-metadata-reader.svg?branch=master)](https://travis-ci.org/shutterstock/orc-metadata-reader)
![Python version](https://img.shields.io/badge/python-2.7-blue.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Library for reading [ORC](https://orc.apache.org/) metadata in python.



## Install 
```
python setup.py install
```


## Usage

Read a local file.
```python
from orc_metadata.reader import read_metadata

# Read metadata from local ORC file
result = read_metadata('path/to/file.orc', schema=True)
```

Read S3 files.
```python
from orc_metadata.reader import read_metadata_s3

# Read metadata from ORC files in S3
for result in read_metadata_s3('s3_bucket', 'prefix/path/partition=foo/', fetch_size=8000):
    yield result
```
Sample output can be found [here](test/expected_output_json).


### Args

| Name | Default | Meaning |
|--------------|---------|-------------------------------------------------------------------------------------------------------------------------------------|
| schema | False | Get ORC schema. |
| file_stats | False | Get ORC file statistics. |
| stripe_stats | False | Get ORC stripes statistics. |
| stripes | False | Get ORC stripe footer information, requires full file scan. |
| fetch_size | None | Fetch the specified amount of trailing bytes from each file. If `None`, it will fetch the entire file. Only for `read_metadata_s3`. |


#### Note
Reading ORC metadata will not require reading the entire file unless `stripes=True`. When using `read_metadata_s3` you can specify `fetch_size=N` which will only fetch the trailing N bytes from each file in s3.


## Supported compressions
- zlib
- Snappy
- LZO
- LZ4


## Contributing

### Linting
```
flake8 orc_metadata test --ignore=F401 --max-line-length=80
```

### Testing
```
python test/runner.py
```

## Code of Conduct
Please read the [code of conduct](CODE_OF_CONDUCT.md).

## Bugs
Please create an issue using this [template](ISSUE_TEMPLATE.md).
