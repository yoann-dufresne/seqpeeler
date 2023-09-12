from seqpeeler.filemanager import ExperimentDirectory


class Job:
    """
    A class that wrap everything that is needed to run a job

        Attributes:
            exp_dir (ExperimentDirectory): Root directory for the job
            exp_content (ExperimentContent): Files used for the expriment
            initial_cmd (string): Command to run. [before being modified to run in the right directory]
    """

    def __init__(self, exp_content, command):
        self.exp_content = exp_content
        self.initial_cmd = command
        
        self.cmd = None
        self.exp_dir = None
        self.stdout = None
        self.stderr = None

        self.exp_infile_names = {}
        self.exp_outfile_names = {}


    def clean(self):
        if path.exists(self.result_dir):
            rmtree(self.result_dir)

        self.cmd = None
        self.exp_dir = None
        self.stdout = None
        self.stderr = None

        self.exp_infile_names = {}
        self.exp_outfile_names = {}


    def prepare_exec(self, result_dir):
        # Init the command
        self.cmd = self.initial_cmd

        # Create the running dir
        self.exp_dir = ExperimentDirectory(result_dir)
        parentdir = self.exp_dir.dirpath
        
        # Create the input files
        for cmd_filepath in self.exp_content.input_sequences:
            file_manager = self.exp_content.output_files[cmd_filepath]
            # Extract the name of the outfile
            filename = path.basename(file_manager.path)
            # manage name collisions
            idx=0
            while path.exists(path.join(parentdir, filename)):
                filename = basename + f"_{idx}"
                idx += 1
            # register the new path
            newpath = path.join(parentdir, filename)
            self.exp_infile_names[cmd_filepath] = newpath
            self.cmd.replace(cmd_filepath, newpath)
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
            newpath = path.join(dirpath, filename)
            self.exp_outfile_names[cmd_filepath] = newpath
            self.cmd.replace(cmd_filepath, newpath)



# class Scheduler:
