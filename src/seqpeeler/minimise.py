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

            print("before", mainscheduler.waiting_list)
            for j in job.next_jobs(present_behaviour=present_behaviour):
                mainscheduler.submit_job(j)
            print("after", mainscheduler.waiting_list)


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
            # Dichotomic jobs
            for job in create_next_jobs(self):
                yield job
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
                # Dichotomic jobs
                for job in create_next_jobs(self):
                    yield job
                return

            if present_behaviour:
                for i in range(self.deletion_idx, len(self.exp_content.input_sequences)):
                    new_job = DeleteFilesJob(self.exp_content, self.initial_cmd, self.result_dir,
                                                deletion_idx=i)
                    new_job.set_subsumed_job(self)
                    self.children_jobs.append(new_job)
                    yield new_job


def next_mask(content):
    """
    Selectes the largest mask in a file manager and return its coordinates
    """
    max_size = 0
    max_lst = -1
    max_mask = None

    for fm in content.input_sequences.values():
        for lst_idx, mask in fm.get_masks():
            mask_size = mask[1] - mask[0] + 1
            if mask_size > max_size:
                max_size = mask_size
                max_lst = lst_idx
                max_mask = mask

    return fm.original_name, max_lst, max_mask

def create_next_jobs(prev_job):
    """
    Explore the masks of a job to create the next jobs. Returns the jobs created along the process.
    """
    prev_content = prev_job.exp_content

    if prev_content.num_masks() > 0:
        filename, lst_idx, mask = next_mask(prev_content)
        print(filename, lst_idx, mask)
        if mask is not None:
            if mask[2] == SequenceStatus.Dichotomy:
                for job in create_dycho_jobs(prev_job, filename, lst_idx, mask):
                    yield job
            else:
                for job in create_peel_jobs(prev_job, filename, lst_idx, mask):
                    yield job

    return

def create_dycho_jobs(prev_job, filename, lst_to_modify, mask_to_use):
    jobs = []
    lst = prev_job.exp_content.input_sequences[filename].sequence_lists[lst_to_modify]
    left_lst, right_lst = lst.split_dicho_mask(mask_to_use)

    # Left dychotomic job
    left_content = prev_job.exp_content.copy()
    left_content.input_sequences[filename].sequence_lists[lst_to_modify] = left_lst
    left_job = MaskJob(left_content, prev_job.initial_cmd, prev_job.result_dir)
    left_job.set_subsumed_job(prev_job)

    # right dichotomic job
    right_content = prev_job.exp_content.copy()
    right_content.input_sequences[filename].sequence_lists[lst_to_modify] = right_lst
    right_job = MaskJob(right_content, prev_job.initial_cmd, prev_job.result_dir)
    right_job.set_subsumed_job(prev_job)
    left_job.set_subsumed_job(right_job)
    
    # Create a new content to transform dicho mask to peel masks
    fake_content = prev_job.exp_content.copy()
    fake_lst = fake_content.input_sequences[filename].sequence_lists[lst_to_modify]
    mask_idx = fake_lst.masks.index(mask_to_use)
    fake_lst.dicho_to_peel(mask_to_use)
    tmp_mask = fake_lst.masks[mask_idx]
    # Creates a fake job to recursively call the job creation
    fake_job = MaskJob(fake_content, prev_job.initial_cmd, prev_job.result_dir)
    success_peel_job, error_peel_job = create_peel_jobs(fake_job, filename, lst_to_modify, tmp_mask)
    # Dicho jobs have priority on peeling jobs
    left_job.set_subsumed_job(success_peel_job)
    left_job.set_subsumed_job(error_peel_job)
    right_job.set_subsumed_job(success_peel_job)
    right_job.set_subsumed_job(error_peel_job)

    return left_job, right_job, success_peel_job, error_peel_job


def create_peel_jobs(prev_job, filename, lst_to_modify, mask_to_use):
    lst = prev_job.exp_content.input_sequences[filename].sequence_lists[lst_to_modify]
    success_lst, error_lst = lst.split_peel(mask_to_use)

    success_content = prev_job.exp_content.copy()
    success_content.input_sequences[filename].sequence_lists[lst_to_modify] = success_lst
    success_job = MaskJob(success_content, prev_job.initial_cmd, prev_job.result_dir)

    error_content = prev_job.exp_content.copy()
    error_content.input_sequences[filename].sequence_lists[lst_to_modify] = error_lst
    error_job = MaskJob(error_content, prev_job.initial_cmd, prev_job.result_dir)
    success_job.set_subsumed_job(error_job)

    return success_job, error_job


class MaskJob(Job):
    def __init__(self, exp_content, cmd, result_dir):
        super().__init__(exp_content, cmd, result_dir)

    def __repr__(self):
        masks_repr = []
        for fm in self.exp_content.input_sequences.values():
            for lst_idx, mask in fm.get_masks():
                m_type = mask[2].name[0]
                masks_repr.append(f"[{mask[0]}-{mask[1]} {m_type}]")
            masks_repr.append(" ")
        
        return f"{super().__repr__()} {';'.join(masks_repr)}"

    def next_jobs(self, present_behaviour=None):
        for job in create_next_jobs(self):
            self.children_jobs.append(job)
            yield job
