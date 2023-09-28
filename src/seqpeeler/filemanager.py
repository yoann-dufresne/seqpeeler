from os import path, mkdir, SEEK_SET
from sys import stderr
from shutil import rmtree


class SequenceHolder:
    """ Remembers the absolute positions of a sequence in a file (in Bytes)
    """
    def __init__(self, header, left, right, file):
        self.header = header
        self.left = left # First byte of the sequence
        self.right = right # Last byte of the sequence
        self.file = file

    def size(self):
        return self.right - self.left + 1

    def left_split(self, position):
        return SequenceHolder(self.header, self.left, middle, self.file)

    def right_split(self, position):
        return SequenceHolder(self.header, middle, self.right, self.file)

    def divide(self, position):
        if not (self.left <= position <= self.right):
            raise IndexError("Division out of sequence")

        middle = (self.right + self.left) // 2
        return [self.left_split(), self.right_split()]

    def create_header(self):
        return f"{self.header} $$$ left={self.left} right={self.right}"

    def copy(self):
        return SequenceHolder(self.header, self.left, self.right, self.file)

    def __eq__(self, other):
        return self.size() == other.size()

    def __lt__(self, other):
        return self.size() > other.size()

    def __hash__(self):
        return (self.left, self.right).__hash__()

    def __repr__(self):
        return f"({self.left} : {self.right})"


class FileManager:
    def __init__(self, filename):
        # Managing file absence
        if not path.isfile(filename):
            print(f"{filename} does not exists", file=stderr)
            raise FileNotFoundError(filename)

        # Transform file path to absolute
        self.original_name = filename
        self.filename = path.abspath(filename)
        self.sequence_list = []
        self.sequence_cumulative_size = []
        self.total_seq_size = 0
        self.verbose = False


    def _find_position(self, position):
        """ Compute the sequence in which there is the requested position.

            Parameters:
                position: Position inside of the sequence

            Return:
                Index of the sequence inside of the file
        """
        # If first sequence
        if position < self.sequence_cumulative_size[0]:
            return 0

        # Dichotomic search
        left = 1, right = len(self.sequence_list)-1
        while True:
            middle = (left + right) // 2
            # Is it the middle ?
            if self.sequence_cumulative_size[middle-1] <= position < self.sequence_cumulative_size[middle]:
                return middle
            elif position < self.sequence_cumulative_size[middle-1]:
                right = middle - 1
            elif position >= self.sequence_cumulative_size[middle]:
                left = middle + 1


    def file_split(self, position=None):
        """
        Perform a split calculation of the file and return 3 possibilities: left part, right part, both parts with the middle sequence splitted into 2 sequences
        """
        # If no position is given, compute the middle of the file as position
        if position is None:
            position = self.total_seq_size // 2
        elif not (0 <= position < self.total_seq_size):
            raise IndexError(f"Position {position} out of file {self.filename}")

        middle_seq_idx = self._find_position(position)
        middle_seq_offset = position
        middle_cumul_size = self.sequence_cumulative_size[middle_seq_idx]
        if middle_seq_idx != 0:
            middle_seq_offset -= self.sequence_cumulative_size[middle_seq_idx-1]

        splits = [FileManager(self.filename) for _ in range(3)]
        for fm in splits:
            sm.verbose = self.verbose
            sm.original_name = self.original_name
        left_split, right_split, complete_split = splits

        # Create left split
        if middle_seq_idx != 0:
            left_split.sequence_list = self.sequence_list[:middle_seq_idx]
            left_split.sequence_cumulative_size = self.sequence_cumulative_size[:middle_seq_idx]
            left_split.total_seq_size = self.sequence_cumulative_size[middle_seq_idx-1]

        # Split the middle sequence
        middle_seq = self.sequence_list[middle_seq_idx]
        if middle_seq.size() > 1:
            middle_left, middle_right = middle_seq.divide()
            
            left_split.sequence_list.append(middle_left)
            left_split.sequence_cumulative_size.append(left_split.total_seq_size + middle_left.size())
            left_split.total_seq_size = left_split.sequence_cumulative_size[-1]

            right_split.sequence_list.append(middle_right)
            right_split.sequence_cumulative_size.append(middle_right.size())
            right_split.total_seq_size = middle_right.size()

        # Create right split
        if middle_seq_idx != len(self.sequence_list)-1:
            right_split.sequence_list += self.sequence_list[middle_seq_idx+1:]
            right_split.sequence_cumulative_size = 
                ([right_split.total_seq_size] if len(right_split.sequence_list) > 0 else [])
                + [x - middle_cumul_size + right_split.total_seq_size
                    for x in self.sequence_cumulative_size[middle_seq_idx+1:]]
            right_split.total_seq_size = self.sequence_cumulative_size[-1]

        # Create a full file with the central sequence splitted
        complete_split.sequence_list = left_split.sequence_list + right_split.sequence_list
        complete_split.sequence_cumulative_size =
            [x for x in left_split.sequence_cumulative_size]
            + [x + left_split.sequence_cumulative_size[-1] for x in right_split.sequence_cumulative_size]
        complete_split.total_seq_size = complete_split.sequence_cumulative_size[-1]

        return splits


    def __lt__(self, other):
        return self.total_seq_size < other.total_seq_size


    def __repr__(self):
        if self.verbose:
            s = f"FileManager({self.filename}):\n"
            s += '\n'.join([f"\t{seq_hold.create_header()}: {str(seq_hold)}" for seq_hold in self.sequence_list])
            return s
        else:
            return f"FileManager({self.filename}): {len(self.sequence_list)} indexed sequences"


    def _create_holder(self, header, seqstart, filepos):
        if header is not None:
            seq_holder = SequenceHolder(header, seqstart, filepos-1, self)
            self.sequence_list.append(seq_holder)
            self.total_seq_size += seq_holder.size()
            self.sequence_cumulative_size.append(self.total_seq_size)


    def index_sequences(self):
        """ Read the positions of the sequences in the file.
        """
        self.sequence_list = []

        with open(self.filename) as f:
            header = None
            seqstart = 0
            filepos = 0

            for line in f:
                # new header
                if line[0] == '>':
                    # register previous seq
                    self._create_holder(header, seqstart, filepos)

                    # set new seq values
                    header = line[1:].strip()
                    self.sequence_list.append(header)
                    seqstart = filepos + len(line)
                
                filepos += len(line)

            # last sequence
            self._create_holder(header, seqstart, filepos)


    def copy(self):
        copy = FileManager(self.filename)
        
        copy.sequence_list = [x for x in self.sequence_cumulative_size]
        copy.sequence_cumulative_size = [x for x in self.sequence_cumulative_size]

        copy.total_seq_size = self.total_seq_size
        copy.verbose = self.verbose


    def extract(self, dest_file):
        """
        Extract sequences subparts from the origin file to generate the current file

            Parameters:
                dest_file (string): Path to the file to generate
        """
        
        with open(dest_file, "w") as extract, open(self.filename, "rb") as origin:
            # Writes each sub-sequence of the file
            for seq_name in self.sequence_list:
                seq_holder = self.index[seq_name]
                # Write header
                print(f"> {seq_name}_{seq_holder.left}-{seq_holder.right}", file=extract)

                # Write sequence
                origin.seek(seq_holder.left, SEEK_SET)
                to_read = seq_holder.size()
                last_char_is_return = False
                while to_read > 0:
                    # Read by chunk to avoir large RAM
                    read_size = min(to_read, 4194304) # 4 MB max
                    seq_slice = origin.read(read_size)
                    seq_slice = seq_slice.decode('ascii')

                    # Write the slice
                    print(seq_slice, file=extract, end='')
                    last_char_is_return = True if seq_slice[-1] == "\n" else False

                    # Update the remaining size to read
                    to_read -= read_size

                if not last_char_is_return:
                    print("", file=extract)



class ExperimentContent:
    """
    A class that contains all the files needed for an experiment

        Attributes:
            input_sequences (Dict<string, FileManager>): Dict that matches input names from the command line to their corresponding file manager.
            output_files (dict<string, string>): Dict of output names from the command line linked with their absolute path.

    """
    def __init__(self):
        self.ordered_inputs = []
        self.input_sequences = {}
        self.output_files = {}
        self.inputs_size = 0

    def replace_input(self, file_manager, input_idx=0):
        if input_idx >= len(self.input_sequences):
            raise IndexError(f"Too few files for index {input_idx}")

        # remove old file
        old_fm = self.input_sequences.pop(self.ordered_inputs[input_idx])
        self.inputs_size -= old_fm.total_seq_size
        # add new file
        self.input_sequences[file_manager.original_name] = file_manager
        self.ordered_inputs[input_idx] = file_manager.original_name
        self.inputs_size += file_manager.total_seq_size

    def set_input(self, file_manager):
        self.ordered_inputs.append(file_manager.original_name)
        self.input_sequences[file_manager.original_name] = file_manager
        self.inputs_size += file_manager.total_seq_size

    def set_inputs(self, file_managers):
        for fm in file_managers:
            self.set_input(fm)

    def set_outputs(self, paths):
        for p in paths:
            self.output_files[p] = path.abspath(p)

    def size(self):
        return self.inputs_size

    def copy(self):
        copy = ExperimentContent()

        copy.input_sequences = {name: self.input_sequences[name].copy() for name in self.input_sequences}
        copy.output_files = {x: y for x, y in self.output_files.items()}

        return copy


class ExperimentDirectory:
    """ Experiment Directory manager. Will copy, erase and keep track of files for one experiment
    """

    def next_dirname(parentpath):
        """ Return an available directory name for the directory parentpath (not thread safe)
        """
        # verify existance of parent directory
        if not path.isdir(parentpath):
            raise FileNotFoundError(parentpath)
        
        idx = 0
        while True:
            if not path.exists(path.join(parentpath, str(idx))):
                return str(idx)
            idx += 1


    def __init__(self, parentpath, dirname=None, delete_previous=True):
        if dirname is None:
            dirname = ExperimentDirectory.next_dirname(parentpath)

        # verify existance of parent directory
        if not path.isdir(parentpath):
            raise FileNotFoundError(parentpath)
        self.parentpath = path.abspath(parentpath)
        self.dirpath = path.join(self.parentpath, dirname)

        # Verify existance of directory
        if path.isdir(self.dirpath):
            if delete_previous:
                rmtree(self.dirpath)
            else:
                raise FileExistsError(self.dirpath)
        # create the experiment directory
        mkdir(self.dirpath)


    def save_cmd(self, cmd):
        with open(path.join(self.dirpath, ".cmd.txt"), 'w') as fw:
            print(cmd, file=fw)


    def save_outputs(self, return_code, stdout, stderr):
        with open(path.join(self.dirpath, ".returncode.txt"), 'w') as fw:
            print(str(return_code), file=fw)
        with open(path.join(self.dirpath, ".stdout.txt"), 'w') as fw:
            print(stdout, file=fw)
        with open(path.join(self.dirpath, ".stderr.txt"), 'w') as fw:
            print(stderr, file=fw)


    def clean(self):
        rmtree(self.dirpath)

            
