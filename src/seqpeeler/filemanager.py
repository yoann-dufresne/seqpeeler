from enum import Enum
from os import path, mkdir, SEEK_SET
from sys import stderr
from shutil import rmtree
from copy import copy


class SequenceStatus(Enum):
    Dichotmy = 0
    LeftPeel = 1
    RightPeel = 2

class SequenceList:
    def __init__(self):
        self.seq_lists = []
        self.cumulative_size = []
        self.current_iterator = None
        self.status = None

    def copy(self):
        cpy = SequenceList()
        cpy.seq_lists = [x.copy() for x in self.seq_lists]
        cpy.cumulative_size = [x for x in self.cumulative_size]
        cpy.status = self.status
        return cpy

    def __len__(self):
        return sum(len(x) for x in self.seq_lists)

    def __repr__(self):
        return ", ".join(str(x) for x in self.seq_lists)

    def __iter__(self):
        self.iter_idx = 0
        return self

    def __next__(self):
        if 0 <= self.iter_idx < len(self.seq_lists):
            # Init sub-iterator if needed
            if self.current_iterator is None:
                self.current_iterator = self.seq_lists[self.iter_idx].__iter__()
            try:
                # Iterate the sub-iterator
                return self.current_iterator.__next__()
            except StopIteration:
                # Iterate current iterator and retry
                self.iter_idx += 1
                self.current_iterator = None
                return self.__next__()
        else:
            # Stop iteration at the end of the list
            raise StopIteration

    def nucl_size(self):
        if len(self.seq_lists) == 0:
            return 0

        return self.cumulative_size[-1]


    def add_sequence_list(self, seq_lst_obj):
        if seq_lst_obj is None:
            return
        self.cumulative_size.append(self.nucl_size() + seq_lst_obj.nucl_size())
        self.seq_lists.append(seq_lst_obj)

    def add_sequence_holder(self, seq_holder):
        self.add_sequence_list(seq_holder)

    def split(self, position):
        split_lst_found = False
        left, right = 0, len(self.seq_lists) - 1
        middle = (left + right) // 2
        lst_to_split = None

        if position == 0:
            return None, self.copy()
        elif position == self.nucl_size():
            return self.copy(), None

        # Search for the list to split (Dichotomic)
        while not split_lst_found:
            middle = (right + left) // 2
            lst_left_size = 0 if middle == 0 else self.cumulative_size[middle-1]
            lst_right_size = self.cumulative_size[middle]

            if lst_left_size <= position < lst_right_size:
                split_lst_found = True
                lst_to_split = self.seq_lists[middle]
            elif lst_left_size > position:
                right = middle - 1
            else:
                left = middle + 1

        # split the middle list and creates 2 sublists
        middle_presize = self.cumulative_size[middle-1] if middle != 0 else 0
        left_split, right_split = lst_to_split.split(position - middle_presize)

        left_list = SequenceList()
        # before the middle list
        left_list.seq_lists.extend(self.seq_lists[:middle])
        left_list.cumulative_size.extend(self.cumulative_size[:middle])
        # left part of the middle list
        left_list.add_sequence_list(left_split)

        right_list = SequenceList()
        # right par of the middle list
        right_list.add_sequence_list(right_split)
        # after the middle list
        right_list.seq_lists.extend(self.seq_lists[middle+1:])
        size_modifier = self.cumulative_size[middle] - right_split.nucl_size()
        right_list.cumulative_size.extend(x - size_modifier for x in self.cumulative_size[middle+1:])

        # Depth reduction step
        while type(left_list) == SequenceList and len(left_list) == 1:
            left_list = left_list.seq_lists[0]

        while type(right_list) == SequenceList and len(right_list) == 1:
            right_list = right_list.seq_lists[0]

        return left_list, right_list


class SequenceHolder(SequenceList):
    """ Remembers the absolute positions of a sequence in a file (in Bytes)
    """
    def __init__(self, header, left, right, file, masks=None):
        self.header = header
        self.left = left # First byte of the sequence
        self.right = right # Last byte of the sequence
        self.file = file
        if masks is None:
            self.masks = [(left, right, SequenceStatus.Dichotmy)]
        else:
            self.masks = [(x, y, z) for x, y, z in masks]

    def __len__(self):
        return 1

    def __iter__(self):
        self.iterable = True
        return self

    def __next__(self):
        if self.iterable:
            self.iterable = False
            return self
        else:
            raise StopIteration

    def nucl_size(self, unmasked=False):
        return self.right - self.left + 1

    def left_split(self, size):
        left = SequenceHolder(self.header, self.left, self.left+size-1, self.file)
        if left.nucl_size() <= 0 or left.right > self.right:
            return None
        else:
            return left

    def right_split(self, size):
        right = SequenceHolder(self.header, self.right-size+1, self.right, self.file)
        if right.left < 0 or right.left > self.right:
            return None
        else:
            return right

    def split(self, position):
        left = self.left_split(position) if position > 0 else None
        right = self.right_split(self.nucl_size() - position) if position < self.right+1 else None
        return left, right

    def create_header(self):
        return f"{self.header} $$$ left={self.left} right={self.right}"

    def copy(self):
        return SequenceHolder(self.header, self.left, self.right, self.file, masks=self.masks)

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

            
