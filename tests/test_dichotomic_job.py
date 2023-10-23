from filemanager import FileManager, ExperimentalContent
from minimize import DichotomicJob


class TestDicho:

	def init(self):
        self.input_filename = "tests/triplets.fa"
        self.input_manager = FileManager(set_input.input_filename)
        self.exp_content = ExperimentalContent()
        self.exp_content.set_input(sequence_holder.input_manager)
        self.cmd = f"python3 tests/triplets_multifasta.py {self.input_filename}"
        self.exp_outdir = "tests_tmp_outdir"

    def test_basic_dicho(self):
        self.init()
        job = DichotomicJob(self.exp_content, self.cmd, self.exp_outdir)
