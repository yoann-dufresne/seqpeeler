from seqpeeler.filemanager import FileManager, ExperimentContent, SequenceStatus
from seqpeeler.minimise import LeftDichoJob, RightDichoJob, LeftPeelJob, RightPeelJob
from shutil import rmtree
from os import path, mkdir

class TestLeftDicho:

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
        self.to_reduce = (self.input_manager, 0, self.mask)
        self.job = LeftDichoJob(self.exp_content, self.cmd, self.exp_outdir, self.to_reduce)

    def test_leftdicho_init(self):
        self.init()

        # files
        assert len(self.job.exp_content.input_sequences) == 1
        in_file = self.job.exp_content.input_sequences[self.input_filename]
        assert in_file.original_name == self.input_manager.original_name
        # Sequences
        left_size = self.input_manager.nucl_size() // 2
        assert len(in_file.sequence_lists) == 1
        assert in_file.nucl_size() == left_size
        # Masks
        seq_lst = in_file.sequence_lists[0]
        assert len(seq_lst.masks) == 1
        assert seq_lst.masks[0] == (0, left_size-1, SequenceStatus.LeftDicho)
        # job status
        assert self.job.file_to_reduce.original_name == in_file.original_name
        assert self.job.mask_to_reduce == self.masks[0]
        assert self.job.mask_to_reduce_lst_idx == 0

    def test_leftdicho_on_success(self):
        self.init()
        next_jobs = list(self.job.next_jobs(present_behaviour=True))
        assert len(next_jobs) == 1
        
        next_job = next_jobs[0]
        assert type(next_job) == LeftDichoJob
        prev_mask = self.job.mask_to_reduce
        assert next_job.mask_to_reduce == (prev_mask[0], prev_mask[1]//2-1, SequenceStatus.LeftDicho)

    def test_leftdicho_on_failure(self):
        self.init()
        print("next")
        next_jobs = list(self.job.next_jobs(present_behaviour=False))
        print("/next")
        assert len(next_jobs) == 1
        
        next_job = next_jobs[0]
        print(type(self.job), next_jobs)
        assert type(next_job) == RightDichoJob
        prev_mask = self.job.mask_to_reduce
        assert next_job.mask_to_reduce == (prev_mask[0], prev_mask[1], SequenceStatus.RightDicho)

        