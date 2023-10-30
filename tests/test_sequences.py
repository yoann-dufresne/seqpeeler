import pytest

from seqpeeler.filemanager import SequenceList, SequenceHolder, SequenceStatus


class TestSequencesDicho:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 2, 11, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 3, 15, "fake.fa")
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

		for split_position in range(self.seq1.left, self.seq1.right+1):
			left, right = self.seq1.split(split_position)
			
			assert left.nucl_size() == split_position - self.seq1.left
			assert left.left == self.seq1.left
			assert left.right == split_position - 1

			assert right.nucl_size() == self.seq1.right - split_position + 1
			assert right.left == split_position
			assert right.right == self.seq1.right

	def test_divide_list(self):
		self.init()

		for split_position in range(0, self.seq_list.nucl_size()+1):
			left, right = self.seq_list.divide(split_position)
			
			assert left.nucl_size() == split_position
			assert left.nucl_size() + right.nucl_size() == self.seq_list.nucl_size()

			# Asserts on left sublist
			if split_position == 0:
				assert len(left) == 0
			elif split_position <= self.seq1.nucl_size():
				assert len(left) == 1
			else:
				assert len(left) == 2

			# asserts on right sublist
			if split_position < self.seq1.nucl_size():
				assert len(right) == 2
			elif split_position != self.seq_list.nucl_size():
				assert len(right) == 1
			else:
				assert len(right) == 0


	def test_leftdicho_list(self):
		self.init()

		mask = self.seq_list.masks[0]
		print(self.seq_list, mask)
		left, right = self.seq_list.split_dicho_mask(mask)
		print(left, left.masks)
		print(right, right.masks)

		assert left.nucl_size() == 11
		assert len(left) == 2
		assert left.seq_holders[0].left == self.seq1.left
		assert left.seq_holders[0].right == self.seq1.right
		assert left.seq_holders[1].left == self.seq2.left
		assert left.seq_holders[1].right == self.seq2.left
		assert len(left.masks) == 1
		assert left.masks[0] == (0, 10, SequenceStatus.Dichotomy)

		assert right.nucl_size() == 12
		assert len(right) == 1
		assert right.seq_holders[0].left == self.seq2.left + 1
		assert right.seq_holders[0].right == self.seq2.right
		assert len(right.masks) == 1
		assert right.masks[0] == (0, 11, SequenceStatus.Dichotomy)

class TestSequencesPeel:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 1, 9, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 12, 25, "fake.fa")
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
		print(mask)
		self.seq_list.masks = [mask]
		on_succes, on_failure = self.seq_list.split_peel(mask)
		print("success", on_succes)
		print("failure", on_failure)
		
		assert len(on_succes) == 2
		assert len(on_succes.masks) == 1
		assert on_succes.nucl_size() == 17
		assert on_succes.masks[0] == (11, 16, SequenceStatus.RightPeel)

		assert len(on_failure) == 2
		assert len(on_failure.masks) == 1
		assert on_failure.nucl_size() == 23
		assert on_failure.masks[0] == (17, 22, SequenceStatus.RightPeel)

	# def test_lefttpeel(self):
	# 	self.init()

	# 	mask = self.seq_list.masks[1]
	# 	self.seq_list.masks = [mask]
	# 	on_succes, on_failure = self.seq_list.split_peel(mask)
		
	# 	assert len(on_succes) == 2
	# 	assert len(on_succes.masks) == 1
	# 	assert on_succes.nucl_size() == 17
	# 	assert on_succes.masks[0] == (0, 4, SequenceStatus.LeftPeel)

	# 	assert len(on_failure) == 2
	# 	assert len(on_failure.masks) == 1
	# 	assert on_failure.nucl_size() == 23
	# 	assert on_failure.masks[0] == (0, 5, SequenceStatus.LeftPeel)


class TestSequencesSplit:
	def init(self):
		self.seq1 = SequenceHolder("complete 1", 0, 9, "fake.fa")
		self.seq2 = SequenceHolder("complete 2", 0, 12, "fake.fa")
		self.seq_list = SequenceList()
		self.seq_list.add_sequence_holder(self.seq1)
		self.seq_list.add_sequence_holder(self.seq2)

	def test_split_center(self):
		self.init()
		left_slice, right_slice = self.seq_list.split_center(5)

		assert len(left_slice) == 1

		assert len(right_slice) == 2