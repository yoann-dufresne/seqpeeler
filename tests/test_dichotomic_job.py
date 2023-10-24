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

    # def test_leftdicho_init(self):
    #     self.init()

    #     # files
    #     assert len(self.job.exp_content.input_sequences) == 1
    #     in_file = self.job.exp_content.input_sequences[self.input_filename]
    #     assert in_file.original_name == self.input_manager.original_name
    #     # Sequences
    #     assert in_file.nucl_size() == self.input_manager.nucl_size() // 2
    #     # Masks
    #     seq_lst = in_file.sequence_lists[0]
    #     assert len(seq_lst.masks) == 1
    #     # job status
    #     assert self.job.file_to_reduce == in_file
    #     assert self.job.mask_to_reduce == seq_lst.masks[0]
    #     assert self.job.mask_to_reduce_lst_idx == 0

    # def test_basic_dicho_on_success(self):
    #     self.init()
    #     next_jobs = self.job.next_jobs(present_behaviour=True)
    #     assert len(next_jobs) == 2
    #     self.job.clean()
    #     assert False

    # def test_leftdicho_on_failure(self):
    #     self.init()

    #     # job
    #     next_jobs = list(self.job.next_jobs(present_behaviour=False))
    #     assert len(next_jobs) == 1
    #     assert type(next_jobs[0]) == RightDichoJob
    #     # sequences
    #     fm = next_jobs[0].file_to_reduce
    #     print(fm)
    #     assert len(fm) == len(self.input_manager)
    #     assert fm.nucl_size() == self.input_manager.nucl_size()
    #     assert len(fm.sequence_lists) == 1
    #     # Masks
    #     lst = fm.sequence_lists[0]
    #     assert len(lst.masks) == 1
    #     assert lst.masks[0] == (0, 22, SequenceStatus.RightDicho)
        
        