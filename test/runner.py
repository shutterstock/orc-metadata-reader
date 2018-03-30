import pickle as pkl
import unittest
from _orc_metadata import read_metadata, ORCReadException


TEST_CASES = [
    'TestOrcFile.columnProjection',
    'TestOrcFile.metaData',
    'TestOrcFile.emptyFile',
    'TestOrcFile.testMemoryManagementV11',
    'TestOrcFile.testMemoryManagementV12',
    'TestOrcFile.testPredicatePushdown',
    'TestOrcFile.testStringAndBinaryStatistics',
    'TestOrcFile.testTimestamp',
    'TestOrcFile.testUnionAndTimestamp',
    'TestOrcFile.testWithoutIndex',
    'nulls-at-end-snappy',
    'orc-file-11-format',
    'orc_split_elim',
    'version1999',
    'TestOrcFile.test1',
    'TestOrcFile.testDate1900',
    'TestOrcFile.testSeek',
    'TestOrcFile.testStripeLevelStats',
    'TestOrcFile.testDate2038',
    'decimal',
    'demo-11-none',
    'demo-11-zlib',
    'demo-12-zlib',
    'over1k_bloom',
    'TestOrcFile.testSnappy',
    'TestVectorOrcFile.testLz4',
    'TestVectorOrcFile.testLzo',
]


class TestReader(unittest.TestCase):

    def test__TestOrcFile_partial_all(self):
        with self.assertRaises(ORCReadException):
            read_metadata('test/orc_files/TestOrcFile.partial.orc',
                          file_stats=True, stripe_stats=True,
                          stripes=True)

    def test__TestOrcFile_partial_footer_stats(self):
        actual_content = read_metadata('test/orc_files/TestOrcFile.partial.orc',
                                       schema=True, file_stats=True,
                                       stripe_stats=True)

        with open('test/expected_output/TestOrcFile.partial.pkl', 'r') as f:
            expected_content = pkl.loads(f.read())
        self.assertDictEqual(expected_content, actual_content)


def test_file_read(filename):
    def test_expected(self):
        in_file_directory = 'test/orc_files/{f}.orc'
        expected_result_directory = 'test/expected_output/{f}.pkl'

        try:
            actual_content = read_metadata(in_file_directory.format(f=filename),
                                           schema=True, file_stats=True,
                                           stripe_stats=True, stripes=True)
        except ORCReadException:
            self.skipTest("Reader not compiled for this compression.")

        with open(expected_result_directory.format(f=filename), 'r') as f:
            expected_content = pkl.loads(f.read())
        self.assertDictEqual(expected_content, actual_content)
    return test_expected


if __name__ == '__main__':
    for test_case in TEST_CASES:
        filename = test_case
        test_func = test_file_read(filename)
        test_func.__name__ = 'test__%s' % filename.replace('.', '_')
        setattr(TestReader, test_func.__name__, test_func)

    unittest.main()
