from time import sleep
from os import path
from subprocess import Popen, PIPE
from shutil import rmtree

from seqpeeler.filemanager import ExperimentDirectory


class Job:
    """
    A class that wrap everything that is needed to run a job

        Attributes:
            exp_dir (ExperimentDirectory): Root directory for the job
            exp_content (ExperimentContent): Files used for the expriment
            initial_cmd (string): Command to run. [before being modified to run in the right directory]
    """

    job_id = 0

    def next_id():
        nxt = Job.job_id
        Job.job_id += 1
        return nxt


    def __init__(self, exp_content, command, result_dir):
        self.id = Job.next_id()
        self.exp_content = exp_content
        self.initial_cmd = command
        self.result_dir = result_dir
        self.status = "NOT_READY"
        
        self.cmd = None
        self.exp_dir = None
        self.returncode = None
        self.stdout = None
        self.stderr = None

        self.exp_infile_names = {}
        self.exp_outfile_names = {}

        self.child_jobs = []
        self.subsumed_jobs = []


    def save_triggers(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout.read().decode('ascii')
        self.stderr = stderr.read().decode('ascii')
        self.exp_dir.save_outputs(returncode, self.stdout, self.stderr)


    def clean(self, mrproper=False):
        self.exp_dir.clean()

        if mrproper:
            self.cmd = None
            self.exp_dir = None
            self.stdout = None
            self.stderr = None

            self.exp_infile_names = {}
            self.exp_outfile_names = {}
        
        self.status = "CLEANED"


    def set_subsumed_job(self, job):
        self.subsumed_jobs.append(job)


    def prepare_exec(self):
        """
        Setup a directory for the job inside of the result_dir
        """
        # Init the command
        self.cmd = self.initial_cmd

        # Create the running dir
        self.exp_dir = ExperimentDirectory(self.result_dir)
        parentdir = self.exp_dir.dirpath
        
        # Create the input files
        for cmd_input_filename in self.exp_content.input_sequences:
            file_manager = self.exp_content.input_sequences[cmd_input_filename]
            # Extract the name of the outfile
            filename = path.basename(file_manager.filename)
            # manage name collisions
            idx=0
            while path.exists(path.join(parentdir, filename)):
                filename = basename + f"_{idx}"
                idx += 1
            # register the new path
            newpath = path.join(parentdir, filename)
            self.exp_infile_names[cmd_input_filename] = newpath
            self.cmd = self.cmd.replace(cmd_input_filename, newpath)
            # copy the correct slice of the file in the exp directory
            file_manager.extract(newpath)

        # Rename the output files to be generated in the exp directory
        for cmd_filepath in self.exp_content.output_files:
            abs_filepath = self.exp_content.output_files[cmd_filepath]
            # Extract the name of the outfile
            filename = basename = path.basename(abs_filepath)
            # manage name collisions
            idx=0
            while path.exists(path.join(parentdir, filename)):
                filename = basename + f"_{idx}"
                idx += 1
            # register the new path
            newpath = path.join(parentdir, filename)
            self.exp_outfile_names[cmd_filepath] = newpath
            self.cmd.replace(cmd_filepath, newpath)

        self.status = "READY"

    def __hash__(self):
        return self.id

    def __eq__(self, other):
        return other.id == self.id



class Scheduler:
    """
    Run a job set according to their priorities and the number of allowed cores

        Attributes:
            
    """

    def __init__(self):
        self.running_list = []
        self.waiting_list = []
        self.terminated_list = []
        self.canceled_list = []
        self.processes = {}
        self.max_processes = 1
        self.expected_behaviour = (None, None, None)


    def set_expected_job_behaviour(self, returncode, stdout, stderr):
        self.expected_behaviour = (returncode, stdout, stderr)


    def submit_job(self, job):
        self.waiting_list.append(job)


    def terminate_job(self, job):
        process = self.processes[job]
        # Save stdout, stderr and return code
        job.save_triggers(process.returncode, process.stdout, process.stderr)
        
        # Join the process to properly close everything
        self.processes[job].communicate()

        job.status = "TERMINATED"
        self.terminated_list.append(job)

        # Cancel subsumed jobs
        for ss_job in job.subsumed_jobs:
            self.cancel(ss_job)


    def cancel(self, job):
        # Cancel job
        match job.status:
            case "RUNNING":
                process = self.processes[job]
                process.kill()
                self.processes.pop(job)
                self.running_list.remove(job)

            case "NOT_READY" | "READY":
                self.waiting_list.remove(job)

            case "CANCELED":
                return


        job.status = "CANCELED"
        self.canceled_list.append(job)

        # cancel children
        for child in job.child_jobs:
            self.cancel(child)


    def is_running(self):
        return (len(self.waiting_list) > 0) or (len(self.running_list) > 0)


    def keep_running(self):
        # Verify the end of running jobs
        old_running_list = self.running_list
        self.running_list = []
        to_terminate = []
        for job in old_running_list:
            if self.processes[job].poll() is not None:
                # Job is over
                to_terminate.append(job)
            else:
                self.running_list.append(job)

        for j in to_terminate:
            self.terminate_job(job)

        # Do nothing if the max allowed number of precesses are currently running
        if self.max_processes <= len(self.running_list) or len(self.waiting_list) == 0:
            sleep(0.1)
            return

        # Run a new job
        self.start_next_job()


    def start_next_job(self):
        # Select next job to start
        job = self.waiting_list.pop(0)
        job.prepare_exec()

        print(f"-- Running {job.cmd}")
        job.exp_dir.save_cmd(job.cmd)

        # Run the job
        process = Popen(args=job.cmd, stdout=PIPE, stderr=PIPE, shell=True)
        self.running_list.append(job)
        self.processes[job] = process
        job.status = "RUNNING"


    def is_behaviour_present(self, job):
        # Is the expected behaviour present ?
        return_ok = True
        if self.expected_behaviour[0] is not None:
            return_ok = self.expected_behaviour[0] == job.returncode
        stdout_ok = True
        if self.expected_behaviour[1] is not None:
            stdout_ok = self.expected_behaviour[1] in job.stdout
        stderr_ok = True
        if self.expected_behaviour[2] is not None:
            stderr_ok = self.expected_behaviour[2] in job.stderr
        
        # If behaviour present
        return return_ok and stdout_ok and stderr_ok


mainscheduler = Scheduler()

