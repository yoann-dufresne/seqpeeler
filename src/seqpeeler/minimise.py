from enum import Enum
from sys import stderr

from seqpeeler.filemanager import ExperimentDirectory, ExperimentContent
from seqpeeler.processes import Job, mainscheduler


class Peeler:

    def __init__(self, args):
        self.args = args
        self.expected_return = args.returncode
        self.expected_stdout = args.stdout
        self.expected_stderr = args.stderr
        self.expected = (args.returncode, args.stdout, args.stderr)

        self.best_job = None

    
    def reduce_file_set(self, file_managers):
        # Prepare the context of exec
        exp_content = ExperimentContent()
        exp_content.set_inputs(file_managers)
        exp_content.set_outputs(self.args.outfilenames)

        # Create the first job (entire files)
        job = CompleteJob(exp_content, self.args.command_line, self.args.outdir)
        mainscheduler.submit_job(job)
        self.best_job = job

        while mainscheduler.is_running():
            mainscheduler.keep_running()
            self.schedule_next_jobs()

    
    def schedule_next_jobs(self):
        while len(mainscheduler.terminated_list) > 0:
            job = mainscheduler.terminated_list.pop(0)

            if job.exp_content.size() < self.best_job.exp_content.size():
                self.best_job = job

            # TODO: Cancel subsumed jobs

            for j in job.next_jobs():
                mainscheduler.submit_job(j)


class CompleteJob(Job):
    """
    Job that will run the command on the entire file set to make sure that the behaviour is present
    """

    def __init__(self, exp_content, cmd, out_directory):
        super().__init__(exp_content, cmd, out_directory)


    def next_jobs(self):
        # Expected behaviour is not present
        if not mainscheduler.is_behaviour_present(self):
            return []

        # Expected behaviour is present

        # Only 1 file => Try to reduce its sequences
        if len(self.exp_content.input_sequences) == 1:
            print("TODO: dichotomic Job")
            return []

        # Multiple files => Try to remove some files
        in_files = list(self.exp_content.input_sequences.values())
        in_files.sort(reverse=True)

        # Experiment content with ordered input file
        content = ExperimentContent()
        content.set_outputs(self.exp_content.output_files.keys())
        content.set_inputs(in_files)

        self.child_jobs = []
        for idx in range(len(in_files)):
            # One delete job for each possible file
            new_job = DeleteFilesJob(content, self.initial_cmd, self.result_dir, deletion_idx=idx)

            # Allow cancelation of job x if previous has a result: The previous result will be better due to previous sorting
            for prev_job in self.child_jobs:
                prev_job.set_subsumed_job(new_job)
            new_job.set_subsumed_job(self)

            self.child_jobs.append(new_job)

        return self.child_jobs


class DeleteFilesJob(Job):
    def __init__(self, exp_content, cmd, exp_outdir, deletion_idx=0):
        self.deletion_idx = deletion_idx

        delete_content = ExperimentContent()
        for idx, input_manager in enumerate(exp_content.input_sequences.values()):
            if idx != self.deletion_idx:
                delete_content.set_input(input_manager)
            else:
                cmd = cmd.replace(input_manager.original_name, "").replace("  ", " ")
        delete_content.set_outputs(exp_content.output_files.keys())
        
        super().__init__(delete_content, cmd, exp_outdir)


    def next_jobs(self):
        # Expected behaviour is not present
        if not mainscheduler.is_behaviour_present(self):
            self.clean()
            return []

        sequences = self.exp_content.ordered_inputs
        # Nothing to delete after that job
        if (len(sequences) == 1) or (self.deletion_idx == (len(sequences)-1)):
            return []

        self.child_jobs = []
        for idx in range(self.deletion_idx, len(sequences)):
            new_job = DeleteFilesJob(self.exp_content, self.initial_cmd, self.result_dir, deletion_idx=idx)

            # Allow cancelation of job x if previous has a result: The previous result will be better due to previous sorting
            for prev_job in self.child_jobs:
                prev_job.set_subsumed_job(new_job)
            new_job.set_subsumed_job(self)

            self.child_jobs.append(new_job)
        return self.child_jobs


class DichotomicJob(self):
    def __init__(self):
        pass

    def next_jobs(self):
        pass





# def reduce_uniq_file(experiment, args) :
#     """ Iteratively reduce the sequences size from a uniq file until reaching the minimal example.
            
#             Parameters:
#                 experiment (ExperimentContent): An object containing all the file indirection for the experiment.
#                 args (NameTuple): parsed command line arguments
#     """
    
#     while tmpsubseqs : # while set not empty
        
#         seq = tmpsubseqs.pop() # take an arbitrary sequence of the specie
#         sp.subseqs.remove(seq)
#         (begin, end) = seq

#         middle = (seq[0] + seq[1]) // 2     
#         seq1 = (begin, middle)
#         seq2 = (middle, end)

#         # prepare the files and directories needed to check if they pass the test
#         sp.subseqs.append(seq1)
#         dirname1 = prepare_dir(spbyfile, cmdargs)
#         sp.subseqs.remove(seq1)     

#         sp.subseqs.append(seq2)
#         dirname2 = prepare_dir(spbyfile, cmdargs)

#         sp.subseqs.append(seq1)
#         dirname3 = prepare_dir(spbyfile, cmdargs)
#         sp.subseqs.remove(seq1)
#         sp.subseqs.remove(seq2)

#         dirnames = list()
#         priorities = list()
#         if middle != end :
#             dirnames.append(dirname1)
#             priorities.append(True)
#         if middle != begin :
#             dirnames.append(dirname2)
#             priorities.append(True)
#         if middle != end and middle != begin :
#             dirnames.append(dirname3)
#             priorities.append(False)
        
#         #print("nombre de proc simultannés : ", len(dirnames))
        
#         firstdirname = trigger_and_wait_processes(cmdargs, dirnames, priorities)
#         rmtree(dirname1)
#         rmtree(dirname2)
#         rmtree(dirname3)

#         # TODO : utiliser des elif au lieu des continue ?
#         # case where the target fragment is in the first half
#         if firstdirname is not None and firstdirname == dirname1 :
#             #print("case 1")
#             sp.subseqs.append(seq1)
#             tmpsubseqs.append(seq1)
#             continue
        
#         # case where the target fragment is in the second half
#         if firstdirname is not None and firstdirname == dirname2 :
#             #print("case 2")
#             sp.subseqs.append(seq2)
#             tmpsubseqs.append(seq2)
#             continue
        
#         # case where there are two co-factor sequences
#         # so we cut the seq in half and add them to the set to be reduced
#         # TODO : relancer le check_output pour trouver les co-factors qui ne sont pas de part et d'autre du milieu de la séquence
#         if firstdirname is not None and firstdirname == dirname3 :
#             #print("case 3")
#             sp.subseqs.append(seq1)
#             sp.subseqs.append(seq2)
#             tmpsubseqs.append(seq1)
#             tmpsubseqs.append(seq2)
#             continue
        
#         # case where the target sequence is on both sides of the cut
#         # so we strip first and last unnecessary nucleotids
#         #print("case 4")
#         seq = strip_sequence(seq, sp, spbyfile, True, cmdargs)
#         seq = strip_sequence(seq, sp, spbyfile, False, cmdargs)
#         sp.subseqs.append(seq)
    
#     return None
