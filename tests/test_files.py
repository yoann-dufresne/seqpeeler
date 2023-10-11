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

	# def test_split_first_seq(self):
	# 	fm = FileManager("tests/2triplets.fa")
	# 	fm.index_sequences()
	# 	# print(fm.sequence_list.nucl_size())

	# 	split_position = 3
	# 	left, right = fm.sequence_list.split(split_position)
	# 	assert len(left) == 1
	# 	assert left.nucl_size() == split_position

	# 	assert len(right) == 2
	# 	assert right.nucl_size() == fm.sequence_list.nucl_size() - split_position

	# def test_split_second_seq(self):
	# 	fm = FileManager("tests/2triplets.fa")
	# 	fm.index_sequences()
	# 	# print(fm.sequence_list.nucl_size())

	# 	split_position = 12
	# 	left, right = fm.sequence_list.split(split_position)
	# 	assert len(left) == 2
	# 	assert left.nucl_size() == split_position

	# 	assert len(right) == 1
	# 	assert right.nucl_size() == fm.sequence_list.nucl_size() - split_position

	# def test_split_between_seq(self):
	# 	fm = FileManager("tests/2triplets.fa")
	# 	fm.index_sequences()
	# 	# print(fm.sequence_list.nucl_size())

	# 	split_position = 11
	# 	left, right = fm.sequence_list.split(split_position)
	# 	assert len(left) == 1
	# 	assert left.nucl_size() == split_position

	# 	assert len(right) == 1
	# 	assert right.nucl_size() == fm.sequence_list.nucl_size() - split_position
