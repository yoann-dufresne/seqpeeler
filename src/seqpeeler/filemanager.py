from os import path, mkdir, SEEK_SET
from sys import stderr
from shutil import rmtree


class SequenceHolder:
    """ Remembers the absolute positions of a sequence in a file (in Bytes)
    """
    def __init__(self, left, right):
        self.left = left # First byte of the sequence
        self.right = right # Last byte of the sequence

    def size(self):
        return self.right - self.left + 1

    def copy(self):
        return SequenceHolder(self.left, self.right)

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
        self.index = None
        self.sequence_list = []
        self.total_seq_size = 0
        self.verbose = False


    def __repr__(self):
        if self.verbose:
            s = f"FileManager({self.filename}):\n"
            s += '\n'.join([f"\t{header}: {str(self.index[header])}" for header in self.index])
            return s
        else:
            return f"FileManager({self.filename}): {len(self.index)} indexed sequences"


    def _index_sequence(self, header, seqstart, filepos):
        if header is not None:
            if header in self.index:
                # Multiple identical headers in the file
                print(f"WARNING: header {header} is present more than once in the file {filename}. Only hte last one have been kept", file=stderr)
                self.total_seq_size -= self.index[header].size()
            self.index[header] = SequenceHolder(seqstart, filepos-1)
            self.total_seq_size += self.index[header].size()


    def index_sequences(self):
        """ Index the positions of the sequences in the file.
            TODO: keep track of the sequence order
        """
        self.index = {}

        with open(self.filename) as f:
            header = None
            seqstart = 0
            filepos = 0

            for line in f:
                # new header
                if line[0] == '>':
                    # register previous seq
                    self._index_sequence(header, seqstart, filepos)

                    # set new seq values
                    header = line[1:].strip()
                    self.sequence_list.append(header)
                    seqstart = filepos + len(line)
                
                filepos += len(line)

            # last sequence
            self._index_sequence(header, seqstart, filepos)


    def copy(self):
        copy = FileManager(self.filename)
        
        copy.index = None if self.index is None else {}
        if self.index is not None:
            for name in self.index:
                copy.index[name] = self.index[name].copy()

        copy.total_seq_size = self.total_seq_size
        other.verbose = self.verbose


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

    def copy(self):
        copy = ExperimentContent()

        copy.input_sequences = {name: self.input_sequences[name].copy() for name in self.input_sequences}
        copy.output_files = {x: y for x, y in self.output_files.items()}

        return copy


    def split_inputs(self):
        if self.inputs_size == 1:
            return []

        middle_idx = self.inputs_size // 2
        
        sub_experiments = [ExperimentContent() for _ in range(3)]
        for ec in sub_experiments:
            ec.set_outputs(self.output_files.keys())

        current_idx = 0
        for filename in self.ordered_inputs:
            manager = self.input_sequences[filename]

            # Case 1: File on the left of the break point
            if current_idx + manager.total_seq_size <= middle_idx:
                pass
            # Case 2: File on the right of the break point
            elif current_idx > middle_idx:
                pass
            # Case 3: File including the break point
            else:
                pass


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


    def clean(self):
        rmtree(self.dirpath)

            
