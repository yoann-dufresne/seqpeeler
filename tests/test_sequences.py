import pytest




from seqpeeler.filemanager import SequenceList, SequenceHolder, SequenceStatus

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

	def test_position_split_seq(self):
		self.init()

		for split_position in range(len(self.seq1)+1):
			left, right = self.seq1.split_position(split_position)
			
			assert left.nucl_size() == split_position
			assert left.left == 0
			assert left.right == split_position - 1

			assert right.nucl_size() == self.seq1.nucl_size() - split_position
			assert right.left == split_position
			assert right.right == self.seq1.nucl_size() - 1

	def test_dicho_seq(self):
		self.init()

		mask = self.seq1.masks[0]
		assert mask == (0, 9, SequenceStatus.Dichotomy, self.seq1)
		left, right = self.seq1.split(mask)

		assert left.nucl_size() == 5
		assert left.left == 0
		assert left.right == 4
		assert len(left.masks) == 1
		assert left.masks[0] == (0, 4, SequenceStatus.Dichotomy, left)

		assert right.nucl_size() == 5
		assert right.left == 5
		assert right.right == 9
		assert len(right.masks) == 1
		assert right.masks[0] == (0, 4, SequenceStatus.Dichotomy, right)

	def test_position_split_list_right(self):
		self.init()

		split_position = 11
		left, right = self.seq_list.split_position(split_position)
		assert left.nucl_size() == split_position
		assert right.nucl_size() == self.seq_list.nucl_size() - split_position

	def test_dicho_list(self):
		self.init()

		mask = self.seq_list.masks[0]
		assert mask == (0, 22, SequenceStatus.Dichotomy, self.seq_list)
		left, right = self.seq_list.split(mask)

		assert left.nucl_size() == 11
		assert len(left) == 2
		assert type(left) == SequenceList
		assert left.seq_lists[0].left == 0
		assert left.seq_lists[0].right == 9
		assert left.seq_lists[1].left == 0
		assert left.seq_lists[1].right == 0

		assert len(left.masks) == 1
		assert left.masks[0] == (0, 10, SequenceStatus.Dichotomy, left)

		assert right.nucl_size() == 12
		assert len(right) == 1
		assert type(right) == SequenceHolder
		assert right.left == 1
		assert right.right == 12
		assert len(right.masks) == 1
		assert right.masks[0] == (0, 11, SequenceStatus.Dichotomy, right)


	# def test_dicho_list_left(self):
	# 	self.init()

	# 	split_position = 4
	# 	left, right = self.seq_list.split_position(split_position)
	# 	assert left.nucl_size() == split_position
	# 	assert right.nucl_size() == self.seq_list.nucl_size() - split_position


