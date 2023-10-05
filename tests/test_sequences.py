import pytest




from seqpeeler.filemanager import SequenceList, SequenceHolder

class TestSequences:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 0, 9, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 0, 12, "fake.fa")
		self.seq_list = SequenceList()
		self.seq_list.add_sequence_list(self.seq1)
		self.seq_list.add_sequence_list(self.seq2)

	def test_size(self):
		self.init()
		assert len(self.seq1) == 1
		assert self.seq1.nucl_size() == 10
		assert len(self.seq2) == 1
		assert self.seq2.nucl_size() == 13
		assert len(self.seq_list) == len(self.seq1) + len(self.seq2)
		assert self.seq_list.nucl_size() == self.seq1.nucl_size() + self.seq2.nucl_size()

	def test_dicho_seq(self):
		self.init()

		for split_position in range(len(self.seq1)+1):
			left, right = self.seq1.split(split_position)
			
			if split_position == 0:
				assert left is None
			else:
				assert left.nucl_size() == split_position
				assert left.left == 0
				assert left.right == split_position - 1

			if split_position == self.seq1.nucl_size():
				assert right is None
			else:
				assert right.nucl_size() == self.seq1.nucl_size() - split_position
				assert right.left == split_position
				assert right.right == self.seq1.nucl_size() - 1

	def test_dicho_list_right(self):
		self.init()

		split_position = 11
		left, right = self.seq_list.split(split_position)
		assert left.nucl_size() == split_position
		assert right.nucl_size() == self.seq_list.nucl_size() - split_position

	def test_dicho_list_left(self):
		self.init()

		split_position = 4
		left, right = self.seq_list.split(split_position)
		assert left.nucl_size() == split_position
		assert right.nucl_size() == self.seq_list.nucl_size() - split_position

	def test_dicho_list_0_split(self):
		self.init()

		left, right = self.seq_list.split(0)
		assert left is None
		assert right.nucl_size() == self.seq_list.nucl_size()

	def test_dicho_list_len_split(self):
		self.init()

		left, right = self.seq_list.split(self.seq_list.nucl_size())
		assert left.nucl_size() == self.seq_list.nucl_size()
		assert right is None
