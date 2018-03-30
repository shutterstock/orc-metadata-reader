import platform
from distutils.core import setup, Extension
from ctypes.util import find_library


compression_libraries = ['z', 'snappy', 'lz4', 'lzo']
compression_flags = ['HAS_ZLIB', 'HAS_SNAPPY', 'HAS_LZ4', 'HAS_LZO']
compression_macros = []

libraries = []
for lib, flag in zip(compression_libraries, compression_flags):
    if find_library(lib):
        libraries.append(lib)
        compression_macros.append((flag, '1'))

sources = ["src/third_party/protobuf-c/protobuf-c.c",
           "src/orc-proto/orc.pb-c.c",
           "src/_ext.c"]

include_dirs = ["src/third_party/protobuf-c",
                "src/orc-proto", "src"]

extra_cflags = []
if platform.system() == 'Darwin':
    extra_cflags.append("-Wno-shorten-64-to-32")

ext = Extension("_orc_metadata",
                sources=sources,
                libraries=libraries,
                include_dirs=include_dirs,
                extra_compile_args=extra_cflags,
                define_macros=compression_macros)

setup(name='orc_metadata',
      ext_modules=[ext],
      packages=['orc_metadata'],
      version='0.1',
      description='Library for reading ORC metadata in python.',
      author='Joe Quadrino',
      author_email='jquadrino@shutterstock.com',
      url='https://github.com/shutterstock/orc-metadata',
      keywords=['orc', 'metadata', 'reader', 'hadoop', 's3'],
      license='MIT')
