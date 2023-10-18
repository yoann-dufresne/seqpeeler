import pytest

from seqpeeler.filemanager import SequenceList, SequenceHolder, SequenceStatus


class TestSequencesDicho:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 0, 9, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 0, 12, "fake.fa")
		self.seq_list = SequenceList()
		self.seq_list.add_sequence_holder(self.seq1)
		self.seq_list.add_sequence_holder(self.seq2)
		self.seq_list.init_masks()

	def test_size(self):
		self.init()
		assert len(self.seq1) == 1
		assert self.seq1.nucl_size() == 10
		assert len(self.seq2) == 1
		assert self.seq2.nucl_size() == 13
		assert len(self.seq_list) == len(self.seq1) + len(self.seq2)
		assert self.seq_list.nucl_size() == self.seq1.nucl_size() + self.seq2.nucl_size()
		assert len(self.seq_list.masks) == 1
		assert self.seq_list.masks[0] == (0, 22, SequenceStatus.Dichotomy)

	def test_position_split_seq(self):
		self.init()

		for split_position in range(len(self.seq1)+1):
			left, right = self.seq1.split(split_position)
			
			assert left.nucl_size() == split_position
			assert left.left == 0
			assert left.right == split_position - 1

			assert right.nucl_size() == self.seq1.nucl_size() - split_position
			assert right.left == split_position
			assert right.right == self.seq1.nucl_size() - 1

	def test_dicho_list(self):
		self.init()

		mask = self.seq_list.masks[0]
		left, right = self.seq_list.split(mask)

		assert left.nucl_size() == 11
		assert len(left) == 2
		assert left.seq_holders[0].left == 0
		assert left.seq_holders[0].right == 9
		assert left.seq_holders[1].left == 0
		assert left.seq_holders[1].right == 0

		assert len(left.masks) == 1
		assert left.masks[0] == (0, 10, SequenceStatus.Dichotomy)

		assert right.nucl_size() == 12
		assert len(right) == 1
		assert right.seq_holders[0].left == 1
		assert right.seq_holders[0].right == 12
		assert len(right.masks) == 1
		assert right.masks[0] == (0, 11, SequenceStatus.Dichotomy)

	def test_extends(self):
		self.init()
		lst1 = SequenceList()
		lst1.add_sequence_holder(self.seq1)
		lst1.init_masks()
		lst2 = SequenceList()
		lst2.add_sequence_holder(self.seq2)
		lst2.init_masks()

		lst1.extends(lst2)
		assert len(lst1) == 2
		assert len(lst1.masks) == 2
		assert lst1.masks[0] == (0, 9, SequenceStatus.Dichotomy)
		assert lst1.masks[1] == (10, 22, SequenceStatus.Dichotomy)

class TestSequencesPeel:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 0, 9, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 0, 12, "fake.fa")
		self.seq_list = SequenceList()
		self.seq_list.add_sequence_holder(self.seq1)
		self.seq_list.add_sequence_holder(self.seq2)
		self.seq_list.init_masks()
		self.seq_list.dicho_to_peel(self.seq_list.masks[0])

	def test_masks(self):
		self.init()

		assert len(self.seq_list.masks) == 2
		assert self.seq_list.masks[0] == (11, 22, SequenceStatus.RightPeel)
		assert self.seq_list.masks[1] == (0, 10, SequenceStatus.LeftPeel)

	def test_rightpeel(self):
		self.init()

		mask = self.seq_list.masks[0]
		self.seq_list.masks = [mask]
		on_succes, on_failure = self.seq_list.split(mask)
		
		assert len(on_succes) == 2
		assert len(on_succes.masks) == 1
		assert on_succes.nucl_size() == 17
		assert on_succes.masks[0] == (11, 16, SequenceStatus.RightPeel)

		assert len(on_failure) == 2
		assert len(on_failure.masks) == 1
		assert on_failure.nucl_size() == 23
		assert on_failure.masks[0] == (17, 22, SequenceStatus.RightPeel)

	def test_lefttpeel(self):
		self.init()

		mask = self.seq_list.masks[1]
		self.seq_list.masks = [mask]
		on_succes, on_failure = self.seq_list.split(mask)
		
		assert len(on_succes) == 2
		assert len(on_succes.masks) == 1
		assert on_succes.nucl_size() == 17
		assert on_succes.masks[0] == (0, 4, SequenceStatus.LeftPeel)

		assert len(on_failure) == 2
		assert len(on_failure.masks) == 1
		assert on_failure.nucl_size() == 23
		assert on_failure.masks[0] == (0, 5, SequenceStatus.LeftPeel)
