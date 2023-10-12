import pytest

from seqpeeler.filemanager import FileManager


class TestFileManager:

	def test_sizes(self):
		fm = FileManager("tests/cycle.fa")
		fm.index_sequences()

		assert len(fm) == 1
		assert fm.nucl_size() == 16

		fm = FileManager("tests/2triplets.fa")
		fm.index_sequences()

		assert len(fm) == 2
		assert fm.nucl_size() == 26
