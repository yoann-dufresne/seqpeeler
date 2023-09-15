from time import sleep
from os import path
from subprocess import Popen, PIPE

from seqpeeler.filemanager import ExperimentDirectory


class SequenceJob:
    """
    A class that wrap everything that is needed to run a job

        Attributes:
            exp_dir (ExperimentDirectory): Root directory for the job
            exp_content (ExperimentContent): Files used for the expriment
            initial_cmd (string): Command to run. [before being modified to run in the right directory]
    """

    def __init__(self, exp_content, command, result_dir):
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


    def save_triggers(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout.read().decode('ascii')
        self.stderr = stderr.read().decode('ascii')


    def clean(self):
        if path.exists(self.result_dir):
            rmtree(self.result_dir)

        self.cmd = None
        self.exp_dir = None
        self.stdout = None
        self.stderr = None

        self.exp_infile_names = {}
        self.exp_outfile_names = {}
        
        self.status = "CLEANED"


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
            self.cmd.replace(cmd_input_filename, newpath)
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



class Scheduler:
    """
    Run a job set according to their priorities and the number of allowed cores

        Attributes:
            
    """

    def __init__(self):
        self.running_list = []
        self.waiting_list = []
        self.terminated_list = []
        self.max_processes = 1


    def submit_job(self, job):
        self.waiting_list.append(job)


    def terminate_job(self, process, job):
        # Save stdout, stderr and return code
        job.save_triggers(process.returncode, process.stdout, process.stderr)
        
        # Join the process to properly close everything
        process.communicate()

        job.status = "TERMINATED"
        self.terminated_list.append(job)


    def run(self):
        while len(self.waiting_list) > 0 or len(self.running_list) > 0:
            # Verify the end of running jobs
            new_running_list = []
            for process, job in self.running_list:
                if process.poll() is not None:
                    # Job is over
                    self.terminate_job(process, job)
                else:
                    new_running_list.append((process, job))
            self.running_list = new_running_list

            # Do nothing if the max allowed number of precesses are currently running
            if self.max_processes <= len(self.running_list) or len(self.waiting_list) == 0:
                sleep(1)
                continue

            # Run a new job
            self.start_next_job()


    def start_next_job(self):
        # Select next job to start
        job = self.waiting_list.pop(0)
        job.prepare_exec()

        print(f"Command line to run {job.cmd}")

        # Run the job
        process = Popen(args=job.cmd, stdout=PIPE, stderr=PIPE, shell=True)
        self.running_list.append((process, job))
        job.status = "RUNNING"


mainscheduler = Scheduler()

