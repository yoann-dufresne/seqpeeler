from enum import Enum
from sys import stderr
import heapq

from seqpeeler.filemanager import ExperimentDirectory, ExperimentContent, SequenceStatus
from seqpeeler.processes import Job, mainscheduler


class PeelerArgs:
    def __init__(self):
        self.inputs = []
        self.outputs = []
        self.fof = None
        self.cmd = None

        self.returncode = None
        self.stderr = None
        self.stdout = None

        self.result_dir = None
        self.verbose = False
        self.keep = False

class Peeler:

    def __init__(self, args):
        self.args = args
        self.expected_return = args.returncode
        self.expected_stdout = args.stdout
        self.expected_stderr = args.stderr
        self.expected = (args.returncode, args.stdout, args.stderr)

        self.best_job = None
    
    def reduce_file_set(self):
        file_managers = self.args.inputs
        # Prepare the context of exec
        exp_content = ExperimentContent()
        exp_content.set_inputs(file_managers)
        exp_content.set_outputs(self.args.outputs)

        # Create the first job (entire files)
        job = CompleteJob(exp_content, self.args.cmd, self.args.result_dir)
        mainscheduler.submit_job(job)
        self.best_job = job

        while mainscheduler.is_running():
            mainscheduler.keep_running()
            self.schedule_next_jobs()

    def schedule_next_jobs(self):
        while len(mainscheduler.terminated_list) > 0:
            job = mainscheduler.terminated_list.pop(0)

            present_behaviour = job.is_behaviour_present(*self.expected)
            if present_behaviour:
                # Save best job
                if job.exp_content.size() < self.best_job.exp_content.size():
                    self.best_job = job

                for subsumed in job.subsumed_jobs:
                    mainscheduler.cancel(subsumed)
                    if not self.args.keep:
                        subsumed.clean()
            else:
                for child in job.children_jobs:
                    mainscheduler.cancel(child)
                    if not self.args.keep:
                        child.clean()

            for j in job.next_jobs(present_behaviour=present_behaviour):
                mainscheduler.submit_job(j)


class CompleteJob(Job):
    """
    Job that will run the command on the entire file set to make sure that the behaviour is present
    """

    def __init__(self, exp_content, cmd, result_dir):
        super().__init__(exp_content, cmd, result_dir)

    def next_jobs(self, present_behaviour=False):
        # Expected behaviour is not present
        if not present_behaviour:
            return

        # Only 1 file => Try to reduce its sequences
        if len(self.exp_content.input_sequences) == 1:
            # yield DichotomicJob.from_previous_job(self)
            return

        # Multiple files => Try to remove some files
        in_files = list(self.exp_content.input_sequences.values())
        in_files.sort(reverse=True)

        # Experiment content with ordered input file
        content = ExperimentContent()
        content.set_outputs(self.exp_content.output_files.keys())
        content.set_inputs(in_files)

        for i in range(len(self.exp_content.input_sequences)):
            new_job = DeleteFilesJob(content, self.initial_cmd, self.result_dir, deletion_idx=i)
            new_job.set_subsumed_job(self)
            self.children_jobs.append(new_job)
            yield new_job

        return


class DeleteFilesJob(Job):
    def __init__(self, exp_content, cmd, result_dir, deletion_idx=0):
        self.deletion_idx = deletion_idx
        self.parent_content = exp_content
        self.parent_cmd = cmd

        delete_content = ExperimentContent()
        for idx, input_manager in enumerate(exp_content.input_sequences.values()):
            if idx != self.deletion_idx:
                delete_content.set_input(input_manager)
            else:
                cmd = cmd.replace(input_manager.original_name, "").replace("  ", " ")
        delete_content.set_outputs(exp_content.output_files.keys())
        
        super().__init__(delete_content, cmd, result_dir)

    def next_jobs(self, present_behaviour=False):
        new_job = None
        # Expected behaviour is not present
        if not present_behaviour:
            return

        # Expected behaviour present
        else:
            sequences = self.exp_content.ordered_inputs
            # Nothing to delete after that job
            if len(sequences) == 1 or len(self.exp_content.ordered_inputs) == self.deletion_idx:
                # return DichotomicJob.from_previous_job(self)
                return

            if present_behaviour:
                for i in range(self.deletion_idx, len(self.exp_content.input_sequences)):
                    new_job = DeleteFilesJob(self.exp_content, self.initial_cmd, self.result_dir,
                                                deletion_idx=i)
                    new_job.set_subsumed_job(self)
                    self.children_jobs.append(new_job)
                    yield new_job


class DichotomicJob(Job):

    def __init__(self, exp_content, cmd, result_dir, to_reduce):
        """
        Creates a dichotomic job.

            Parameters:
                exp_content (ExperimentContent): File content of the job
                cmd (string): Command to run (before modification)
                result_dir (string): Directory to use for outputs
                to_reduce (tuple): Triplets that store the file to reduce, the list index in which the reduction is done and the mask to use for reduction
        """
        # 1 - Parameter registration
        self.file_to_reduce, self.mask_to_reduce_lst_idx, self.mask_to_reduce = to_reduce
        # 2 - Modification of the inputs to match the reduction
        self.parent_content = exp_content
        exp_content = self.reduce_inputs()
        # 3 - Creation of the job
        super().__init__(exp_content, cmd, result_dir)

    def next_jobs(self, present_behaviour=False):
        self.update_masks(present_behaviour=present_behaviour)
        mask, lst_idx, file = self.select_mask()
        match(mask[2]):
            case SequenceStatus.LeftDicho:
                yield LeftDichoJob(self.exp_content, self.cmd, self.result_dir, (file, lst_idx, mask))
            case SequenceStatus.RightDicho:
                yield RightDichoJob(self.exp_content, self.cmd, self.result_dir, (file, lst_idx, mask))
            case SequenceStatus.LeftPeel:
                yield LeftPeelJob(self.exp_content, self.cmd, self.result_dir, (file, lst_idx, mask))
            case SequenceStatus.RightPeel:
                yield RightPeelJob(self.exp_content, self.cmd, self.result_dir, (file, lst_idx, mask))

    def select_mask(self):
        mask_to_reduce = None
        mask_to_reduce_lst_idx = None
        file_to_reduce = None
        max_size = 0
        for filename in self.exp_content.input_sequences:
            fm = self.exp_content.input_sequences[filename]
            for lst_idx, mask in fm.get_masks():
                size = mask[1] - mask[0] + 1
                if size > max_size:
                    max_size = size
                    file_to_reduce = fm
                    mask_to_reduce = mask
                    mask_to_reduce_lst_idx = lst_idx
        return mask_to_reduce, mask_to_reduce_lst_idx, file_to_reduce


class LeftDichoJob(DichotomicJob):

    def __init__(self, exp_content, cmd, result_dir, to_reduce):
        super().__init__(exp_content, cmd, result_dir, to_reduce)

    def reduce_inputs(self):
        print("reduce_inputs")
        new_content = self.parent_content.copy()
        new_file = new_content.input_sequences[self.file_to_reduce.original_name]
        print(new_file.nucl_size())
        print("mask", self.mask_to_reduce)

        lst = self.file_to_reduce.sequence_lists[self.mask_to_reduce_lst_idx]
        new_list = lst.split_dicho_mask(self.mask_to_reduce)
        print(new_list)
        print(lst.nucl_size(), new_list.nucl_size())
        new_file.sequence_lists[self.mask_to_reduce_lst_idx] = new_list

        print("/reduce_inputs")
        return new_content

    def update_masks(self, present_behaviour=False):
        lst = self.file_to_reduce.sequence_lists[self.mask_to_reduce_lst_idx]
        mask_idx = lst.masks.index(self.mask_to_reduce)
        lst.masks[mask_idx] = (self.mask_to_reduce[0], self.mask_to_reduce[1], SequenceStatus.RightDicho)


class RightDichoJob(DichotomicJob):

    def __init__(self, exp_content, cmd, result_dir, to_reduce):
        super().__init__(exp_content, cmd, result_dir, to_reduce)

    def reduce_inputs(self):
        return ExperimentContent()

    def update_masks(self, present_behaviour=False):
        pass


class LeftPeelJob(DichotomicJob):

    def __init__(self, exp_content, cmd, result_dir, to_reduce):
        super().__init__(exp_content, cmd, result_dir, to_reduce)

    def reduce_inputs(self):
        return ExperimentContent()

    def update_masks(self, present_behaviour=False):
        pass


class RightPeelJob(DichotomicJob):

    def __init__(self, exp_content, cmd, result_dir, to_reduce):
        super().__init__(exp_content, cmd, result_dir, to_reduce)

    def reduce_inputs(self):
        return ExperimentContent()

    def update_masks(self, present_behaviour=False):
        pass
