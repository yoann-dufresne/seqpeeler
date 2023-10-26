from seqpeeler.filemanager import FileManager, ExperimentContent, SequenceStatus
from seqpeeler.minimise import MaskJob
from shutil import rmtree
from os import path, mkdir

class TestDichotomyJobs:

    def init(self):
        # inputs
        self.input_filename = "tests/triplets.fa"
        self.input_manager = FileManager(self.input_filename)
        self.input_manager.init_content()
        self.lsts = self.input_manager.sequence_lists[0]
        self.masks = self.lsts.masks
        self.exp_content = ExperimentContent()
        self.exp_content.set_input(self.input_manager)
        # cmd
        self.cmd = f"python3 tests/triplets_multifasta.py {self.input_filename}"
        # outputs
        self.exp_outdir = "tests_tmp_outdir"
        self.mask = self.masks[0]
        self.job = MaskJob(self.exp_content, self.cmd, self.exp_outdir)

    def test_init(self):
        self.init()

        # files
        in_file = self.job.exp_content.input_sequences[self.input_filename]
        assert in_file.original_name == self.input_manager.original_name
        # Sequences
        assert len(in_file.sequence_lists) == 1
        seq_lst = in_file.sequence_lists[0]
        assert len(seq_lst) == 2
        assert in_file.nucl_size() == self.input_manager.nucl_size()
        # Masks
        assert len(seq_lst.masks) == 1
        assert seq_lst.masks[0] == (0, self.input_manager.nucl_size()-1, SequenceStatus.Dichotomy)

    def test_next_jobs(self):
        self.init()
        next_jobs = list(self.job.next_jobs(present_behaviour=True))
        assert len(next_jobs) == 4
        
        # left
        left_file = next_jobs[0].exp_content.input_sequences[self.input_manager.original_name]
        left_size = self.input_manager.nucl_size() // 2
        assert left_file.nucl_size() == left_size
        assert len(left_file.sequence_lists) == 1
        left_lst = left_file.sequence_lists[0]
        assert len(left_lst.masks) == 1
        assert left_lst.masks[0] == (0, left_size-1, SequenceStatus.Dichotomy)
        # right
        right_file = next_jobs[1].exp_content.input_sequences[self.input_manager.original_name]
        right_size = self.input_manager.nucl_size() - left_size
        assert right_file.nucl_size() == right_size
        assert len(right_file.sequence_lists) == 1
        right_lst = right_file.sequence_lists[0]
        assert len(right_lst.masks) == 1
        assert right_lst.masks[0] == (0, right_size-1, SequenceStatus.Dichotomy)

class TestPeelingJobs:

    def init(self):
        # inputs
        self.input_filename = "tests/triplets.fa"
        self.input_manager = FileManager(self.input_filename)
        self.input_manager.init_content()
        self.lsts = self.input_manager.sequence_lists[0]
        self.masks = self.lsts.masks
        self.lsts.dicho_to_peel(self.masks[0])
        self.exp_content = ExperimentContent()
        self.exp_content.set_input(self.input_manager)
        # cmd
        self.cmd = f"python3 tests/triplets_multifasta.py {self.input_filename}"
        # outputs
        self.exp_outdir = "tests_tmp_outdir"
        self.mask = self.masks[0]
        self.job = MaskJob(self.exp_content, self.cmd, self.exp_outdir)

    def test_init(self):
        self.init()

        # files
        in_file = self.job.exp_content.input_sequences[self.input_filename]
        assert in_file.original_name == self.input_manager.original_name
        # Sequences
        assert len(in_file.sequence_lists) == 1
        seq_lst = in_file.sequence_lists[0]
        assert len(seq_lst) == 2
        assert in_file.nucl_size() == self.input_manager.nucl_size()
        # Masks
        assert len(seq_lst.masks) == 2
        assert seq_lst.masks[0] == (self.input_manager.nucl_size()//2, self.input_manager.sequence_lists[0].masks[0][1], SequenceStatus.RightPeel)
        assert seq_lst.masks[1] == (0, self.input_manager.nucl_size()//2-1, SequenceStatus.LeftPeel)

    def test_next_jobs(self):
        self.init()
        next_jobs = list(self.job.next_jobs(present_behaviour=True))
        assert len(next_jobs) == 2
        
        # left
        success_file = next_jobs[0].exp_content.input_sequences[self.input_manager.original_name]
        print(success_file.sequence_lists)
        success_size = self.input_manager.nucl_size() // 2
        assert success_file.nucl_size() == success_size
        assert len(success_file.sequence_lists) == 1
        success_lst = success_file.sequence_lists[0]
        assert len(success_lst.masks) == 1
        assert success_lst.masks[0] == (0, success_size-1, SequenceStatus.Dichotomy)
        # right
        error_file = next_jobs[1].exp_content.input_sequences[self.input_manager.original_name]
        error_size = self.input_manager.nucl_size() - success_size
        assert error_file.nucl_size() == error_size
        assert len(error_file.sequence_lists) == 1
        right_lst = error_file.sequence_lists[0]
        assert len(error_lst.masks) == 1
        assert error_lst.masks[0] == (0, error_size-1, SequenceStatus.Dichotomy)