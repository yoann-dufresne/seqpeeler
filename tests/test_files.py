import pytest

from seqpeeler.filemanager import FileManager


class TestFileManager:

	def test_loader(self):
		fm = FileManager("tests/cycle.fa")
		fm.index_sequences()

		assert len(fm.sequence_list) == 1
