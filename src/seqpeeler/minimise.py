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

    def __init__(self, exp_content, cmd, out_directory):
        super().__init__(exp_content, cmd, out_directory)

    def next_jobs(self, present_behaviour=False):
        # Expected behaviour is not present
        if not present_behaviour:
            return []

        # Only 1 file => Try to reduce its sequences
        if len(self.exp_content.input_sequences) == 1:
            return DichotomicJob.from_previous_job(self)

        # Multiple files => Try to remove some files
        in_files = list(self.exp_content.input_sequences.values())
        in_files.sort(reverse=True)

        # Experiment content with ordered input file
        content = ExperimentContent()
        content.set_outputs(self.exp_content.output_files.keys())
        content.set_inputs(in_files)

        new_job = DeleteFilesJob(content, self.initial_cmd, self.result_dir, deletion_idx=0)
        new_job.set_subsumed_job(self)
        self.children_jobs.append(new_job)

        return [new_job]


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
            if len(self.parent_content.ordered_inputs) > 1 and self.deletion_idx < len(self.parent_content.ordered_inputs) - 1:
                new_job = DeleteFilesJob(self.parent_content, self.parent_cmd, self.result_dir,
                                        deletion_idx=self.deletion_idx+1)
                new_job.set_subsumed_job(self)
            else:
                return []

        # Expected behaviour present
        else:
            sequences = self.exp_content.ordered_inputs
            # Nothing to delete after that job
            if len(sequences) == 1 or len(self.exp_content.ordered_inputs) == self.deletion_idx:
                return DichotomicJob.from_previous_job(self)

            if present_behaviour:
                new_job = DeleteFilesJob(self.exp_content, self.initial_cmd, self.result_dir,
                                            deletion_idx=self.deletion_idx)
                new_job.set_subsumed_job(self)
        
        self.children_jobs.append(new_job)
        return [new_job]



class DichotomicJob(Job):
    
    def from_previous_job(job):
        # Generate the priorities if needed
        slice_priorities = []
        if type(job) != DichotomicJob:
            # Init slices
            for in_file in job.exp_content.input_sequences.values():
                for mask in in_file.sequence_list.get_masks():
                    slice_priorities.append(((mask[0] - mask[1]), in_file, mask))
            heapq.heapify(slice_priorities)
        else:
            slice_priorities = job.slice_priorities

        # Select the slice with the highest priority
        _, file, selected_slice = heapq.heappop(slice_priorities)
        _, _, status, seq_list = selected_slice

        if status == SequenceStatus.Dichotomy:
            left, right = seq_list.split(seq_list.nucl_size()//2)
            print("left", left, left.masks)
            # print("right", right, right.masks)

            # Left content prepare
            left_content = job.exp_content.copy()
            left_content.rm_input(file)
            # modify the selected file content TODO TODO
            seq_list = file.sequence_list
            # include the new content into the experiment
            left_content.set_input(file)
            print(left_content)

            # left_job = DichotomicJob()

            # right job

        return []

    def __init__(self, exp_content, cmd, exp_outdir):
        super().__init__(exp_content, cmd, exp_outdir)
        self.slice_priorities = []

    def reduce(self):
        largest_slice = heapq.heappop(self.slices)


    def next_jobs(self, present_behaviour=False):
        return []

        if present_behaviour:
            return DichotomicFileJob.from_other_job(self)
        else:
            return []



