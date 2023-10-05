import pytest

from seqpeeler.filemanager import FileManager


class TestFileManager:

	def test_sizes(self):
		fm = FileManager("tests/cycle.fa")
		fm.index_sequences()

		assert len(fm) == 1
		print(fm.nucl_size())
		assert fm.nucl_size() == 16
