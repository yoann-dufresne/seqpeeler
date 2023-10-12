from enum import Enum
from os import path, mkdir, SEEK_SET
from sys import stderr
from shutil import rmtree
from copy import copy


class SequenceStatus(Enum):
    Dichotomy = 0
    LeftPeel = 1
    RightPeel = 2

class SequenceList:
    def __init__(self):
        self.seq_holders = []
        self.cumulative_size = []
        self.masks = []

    def copy(self):
        cpy = SequenceList()
        cpy.seq_holders = [x.copy() for x in self.seq_holders]
        cpy.cumulative_size = [x for x in self.cumulative_size]
        cpy.masks = [(x, y, z) for x, y, z in self.masks]
        return cpy

    def __len__(self):
        return len(self.seq_holders)

    def __repr__(self):
        return ", ".join(str(x) for x in self.seq_holders)

    def __iter__(self):
        return self

    def __next__(self):
        for x in self.seq_holders:
            yield x
        raise StopIteration

    def nucl_size(self):
        if len(self.seq_holders) == 0:
            return 0

        return self.cumulative_size[-1]

    def get_masks(self):
        for mask in self.masks:
            yield mask

    def init_masks(self):
        self.masks = [(0, self.nucl_size()-1, SequenceStatus.Dichotomy)]

    def add_sequence_holder(self, sequence_holder):
        if sequence_holder is None:
            return

        # size update
        self.cumulative_size.append(self.nucl_size() + sequence_holder.nucl_size())
        # Add the sequence list
        self.seq_holders.append(sequence_holder)

    def split(self, mask):
        """
        WARNING: the split mask must cover 2 positions at least
        """
        if mask not in self.masks:
            raise IndexError("No remaining splitting mask")

        split_position = (mask[0] + mask[1] + 1) // 2

        split_lst_found = False
        holder_to_split = None

        left, middle, right = 0, 0, len(self.seq_holders)
        # Search for the list to split (Dichotomic)
        while not split_lst_found:
            middle = (right + left) // 2
            lst_left_size = 0 if middle == 0 else self.cumulative_size[middle-1]
            lst_right_size = self.cumulative_size[middle]

            if lst_left_size <= split_position < lst_right_size:
                split_lst_found = True
                holder_to_split = self.seq_holders[middle]
            elif lst_left_size > split_position:
                right = middle - 1
            else:
                left = middle + 1

        # split the middle list and creates 2 sublists
        middle_presize = self.cumulative_size[middle-1] if middle != 0 else 0
        left_split, right_split = holder_to_split.split_position(split_position - middle_presize)

        left_list = SequenceList()
        # before the middle list
        for lst in self.seq_holders[:middle]:
            left_list.add_sequence_holder(lst)
        # left part of the middle list
        left_list.add_sequence_holder(left_split)
        if left_list.nucl_size() > 1:
            left_list.init_masks()

        right_list = SequenceList()
        # right par of the middle list
        right_list.add_sequence_holder(right_split)
        # after the middle list
        for lst in self.seq_holders[middle+1:]:
            right_list.add_sequence_holder(lst)
        if right_list.nucl_size() > 1:
            right_list.init_masks()

        return left_list, right_list

    def extends(self, seq_list):
        size_pre_merge = self.nucl_size()

        for seq_holder in seq_list.seq_holders:
            self.add_sequence_holder(seq_holder)
        for start, stop, status in seq_list.masks:
            self.masks.append((start+size_pre_merge, stop+size_pre_merge, status))


class SequenceHolder:
    """ Remembers the absolute positions of a sequence in a file (in Bytes)
    """
    def __init__(self, header, left, right, file):
        self.header = header
        self.left = left # First byte of the sequence
        self.right = right # Last byte of the sequence
        self.file = file

    def __len__(self):
        return 1

    def nucl_size(self, unmasked=False):
        return self.right - self.left + 1

    def split_position(self, position):
        left = SequenceHolder(self.header, self.left, position-1, self.file)
        right = SequenceHolder(self.header, position, self.right, self.file)
        return left, right

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
        self.sequence_list = SequenceList()
        self.verbose = False

    def __len__(self):
        return len(self.sequence_list)

    def nucl_size(self):
        return self.sequence_list.nucl_size()

    def __lt__(self, other):
        return len(self.sequence_list) < len(other.sequence_list)

    def __repr__(self):
        if self.verbose:
            s = f"FileManager({self.filename}):\n"
            s += '\n'.join([f"\t{seq_hold.create_header()}: {str(seq_hold)}" for seq_hold in self.sequence_list])
            return s
        else:
            return f"FileManager({self.filename}): {len(self.sequence_list)} indexed sequences"


    def _register_holder(self, header, seqstart, filepos):
        if header is not None:
            seq_holder = SequenceHolder(header, seqstart, filepos-1, self)
            self.sequence_list.add_sequence_holder(seq_holder)


    def index_sequences(self):
        """ Read the positions of the sequences in the file.
        """
        self.sequence_list = SequenceList()

        with open(self.filename) as f:
            header = None
            seqstart = 0
            filepos = 0

            for line in f:
                # new header
                if line[0] == '>':
                    # register previous seq
                    self._register_holder(header, seqstart, filepos)

                    # set new seq values
                    header = line[1:].strip()
                    seqstart = filepos + len(line)
                
                filepos += len(line)

            # last sequence
            self._register_holder(header, seqstart, filepos)


    def copy(self):
        copy = FileManager(self.filename)
        
        copy.sequence_list = self.sequence_list.copy()
        copy.verbose = self.verbose


    def extract(self, dest_file):
        """
        Extract sequences subparts from the origin file to generate the current file

            Parameters:
                dest_file (string): Path to the file to generate
        """
        
        with open(dest_file, "w") as extract, open(self.filename, "rb") as origin:
            # Writes each sub-sequence of the file
            for seq_holder in self.sequence_list:
                # Write header
                print(f"> {seq_holder.create_header()}", file=extract)

                # Write sequence
                origin.seek(seq_holder.left, SEEK_SET)
                to_read = seq_holder.nucl_size()
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

    def __repr__(self):
        return f"<inputs: {len(self.input_sequences)} ({self.inputs_size}) ; outputs: {len(self.output_files)}>"

    def replace_input(self, file_manager, input_idx=0):
        if input_idx >= len(self.input_sequences):
            raise IndexError(f"Too few files for index {input_idx}")

        # remove old file
        old_fm = self.input_sequences.pop(self.ordered_inputs[input_idx])
        self.inputs_size -= old_fm.nucl_size()
        # add new file
        self.input_sequences[file_manager.original_name] = file_manager
        self.ordered_inputs[input_idx] = file_manager.original_name
        self.inputs_size += file_manager.nucl_size()

    def set_input(self, file_manager):
        self.ordered_inputs.append(file_manager.original_name)
        self.input_sequences[file_manager.original_name] = file_manager
        self.inputs_size += file_manager.nucl_size()

    def rm_input(self, file_manager):
        del self.input_sequences[file_manager.original_name]
        self.ordered_inputs.remove(file_manager.original_name)
        self.inputs_size -= file_manager.nucl_size()

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
        copy.inputs_size = self.inputs_size
        copy.ordered_inputs = [x for x in self.ordered_inputs]

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

            
