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
        self.freezed = False

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
        self.iter_idx = 0
        return self

    def __next__(self):
        if len(self.seq_holders) <= self.iter_idx:
            raise StopIteration

        next_val = self.seq_holders[self.iter_idx]
        self.iter_idx += 1
        return next_val

    def nucl_size(self):
        if len(self.seq_holders) == 0:
            return 0

        return self.cumulative_size[-1]

    def get_masks(self):
        for mask in self.masks:
            yield mask

    def init_masks(self):
        self.masks = [(0, self.nucl_size()-1, SequenceStatus.Dichotomy)]

    def dicho_to_peel(self, mask):
        """
        Transform the mask in parameter into 2 peeler masks of the same size. The mask must be present in the masks list.

        Parameters:
            mask (tuple): The dichotomic mask to transform
        """
        if mask[2] != SequenceStatus.Dichotomy:
            raise ValueError("Wrong mask type")
        maks_position = self.masks.index(mask)

        self.masks.pop(maks_position)
        middle = (mask[1] + mask[0] + 1) // 2
        self.masks.insert(maks_position, (mask[0], middle-1, SequenceStatus.LeftPeel))
        self.masks.insert(maks_position, (middle, mask[1], SequenceStatus.RightPeel))
        
    def add_sequence_holder(self, sequence_holder):
        if sequence_holder is None:
            return

        # size update
        self.cumulative_size.append(self.nucl_size() + sequence_holder.nucl_size())
        # Add the sequence list
        self.seq_holders.append(sequence_holder)

    def split_peel(self, mask):
        if mask not in self.masks:
            raise IndexError("No remaining splitting mask")

        match mask[2]:
            case SequenceStatus.RightPeel:
                return self.split_rightpeel_mask(mask)
            case SequenceStatus.LeftPeel:
                return self.split_leftpeel_mask(mask)

        raise NotImplementedError()

    def get_holder_to_split(self, position):
        split_lst_found = False
        holder_to_split = None

        if self.nucl_size() == position:
            return len(self.seq_holders)-1

        left, middle, right = 0, 0, len(self.seq_holders)
        # Search for the list to split (Dichotomic)
        while not split_lst_found:
            middle = (right + left) // 2
            lst_left_size = 0 if middle == 0 else self.cumulative_size[middle-1]
            lst_right_size = self.cumulative_size[middle]

            if lst_left_size <= position < lst_right_size:
                split_lst_found = True
                holder_to_split = self.seq_holders[middle]
            elif lst_left_size > position:
                right = middle - 1
            else:
                left = middle + 1

        return middle

    def split_rightpeel_mask(self, mask):
        # Get the holder to split
        split_position = (mask[0] + mask[1] + 1) // 2
        holder_idx = self.get_holder_to_split(split_position)
        holder_to_split = self.seq_holders[holder_idx]

        # split the holder to keep the left part
        relative_split_size = split_position - self.cumulative_size[holder_idx-1] if holder_idx != 0 else 0

        print(holder_to_split, relative_split_size)
        left_holder, _ = holder_to_split.split(relative_split_size)
        print(left_holder)

        # Sequence creations
        on_succes = SequenceList()
        on_error = self.copy()
        on_error.masks = []

        for seq_holder in self.seq_holders[:holder_idx]:
            on_succes.add_sequence_holder(seq_holder)
        if left_holder.nucl_size() != 0:
            on_succes.add_sequence_holder(left_holder)

        # Masks update
        position_modifier = mask[1] - split_position
        before_peel_mask = True
        for prev_mask in self.masks:
            if before_peel_mask:
                if mask == prev_mask:
                    before_peel_mask = False
                    on_succes.masks.append((prev_mask[0], split_position-1, prev_mask[2]))
                    on_error.masks.append((split_position, prev_mask[1], prev_mask[2]))
                else:
                    on_succes.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))
                    on_error.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))
            else:
                on_succes.masks.append((prev_mask[0]-position_modifier, prev_mask[1]-position_modifier, prev_mask[2]))
                on_error.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))

        print(on_succes, on_error)
        return on_succes, on_error

    def split_leftpeel_mask(self, mask):
        # Get the holder to split
        split_position = (mask[0] + mask[1] + 1 + 1) // 2
        holder_idx = self.get_holder_to_split(split_position)
        holder_to_split = self.seq_holders[holder_idx]

        # split the holder to keep the left part
        relative_split_size = split_position - self.cumulative_size[holder_idx-1] if holder_idx != 0 else split_position

        _, right_holder = holder_to_split.split(relative_split_size)

        # Sequence creations
        on_succes = SequenceList()
        on_error = self.copy()
        on_error.masks = []

        if right_holder.nucl_size() != 0:
            on_succes.add_sequence_holder(right_holder)
        for seq_holder in self.seq_holders[holder_idx+1:]:
            on_succes.add_sequence_holder(seq_holder)

        # Masks update
        position_modifier = split_position - mask[0]
        before_peel_mask = True
        for prev_mask in self.masks:
            if before_peel_mask:
                if mask == prev_mask:
                    before_peel_mask = False
                    on_succes.masks.append((0, prev_mask[1] - split_position, prev_mask[2]))
                    on_error.masks.append((prev_mask[0], split_position-1, prev_mask[2]))
                else:
                    on_succes.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))
                    on_error.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))
            else:
                on_succes.masks.append((prev_mask[0]-position_modifier, prev_mask[1]-position_modifier, prev_mask[2]))
                on_error.masks.append((prev_mask[0], prev_mask[1], prev_mask[2]))

        print("on_succes", on_succes)
        return on_succes, on_error

    def split_dicho_mask(self, mask):
        """
        WARNING: the split mask must cover 2 positions at least
        """
        split_position = (mask[0] + mask[1] + 1) // 2
        left, right = self.divide(split_position)
        left.init_masks()
        right.init_masks()

        return left, right

    def split_center(self, position):
        left_list, right_list = self.divide(position)
        left_list.masks = [(1, left_list.nucl_size()-1, SequenceStatus.RightPeel)]
        right_list.masks = [(0, left_list.nucl_size()-2, SequenceStatus.LeftPeel)]

        return left_list, right_list

    def divide(self, split_position):
        holder_idx = self.get_holder_to_split(split_position)
        holder_to_split = self.seq_holders[holder_idx]

        # split the middle list and creates 2 sublists
        middle_presize = self.cumulative_size[holder_idx-1] if holder_idx != 0 else 0
        left_split, right_split = holder_to_split.split(split_position - middle_presize + holder_to_split.left)

        left_list = SequenceList()
        # before the middle list
        for lst in self.seq_holders[:holder_idx]:
            left_list.add_sequence_holder(lst)
        # left part of the middle list
        if left_split.nucl_size() > 0:
            left_list.add_sequence_holder(left_split)
    
        right_list = SequenceList()
        # right par of the middle list
        if right_split.nucl_size() > 0:
            right_list.add_sequence_holder(right_split)
        # after the middle list
        for lst in self.seq_holders[holder_idx+1:]:
            right_list.add_sequence_holder(lst)

        return left_list, right_list


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

    def split(self, size):
        left = SequenceHolder(self.header, self.left, size-1, self.file)
        right = SequenceHolder(self.header, size, self.right, self.file)
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
        self.sequence_lists = []
        self.verbose = False

    def __len__(self):
        return sum(len(x) for x in self.sequence_lists)

    def nucl_size(self):
        return sum(x.nucl_size() for x in self.sequence_lists)

    def __lt__(self, other):
        return self.nucl_size() < other.nucl_size()

    def __repr__(self):
        if self.verbose:
            s = f"FileManager({self.filename}):\n"
            for seq_lst in self.sequence_lists:
                s += '['
                s += '\n'.join([f"\t{seq_hold.create_header()}: {str(seq_hold)}" for seq_hold in seq_list])
                s += ']'
            return s
        else:
            return f"FileManager({self.filename}): {len(self)} indexed sequences"

    def num_masks(self):
        return sum(len(lst.masks) for lst in self.sequence_lists)

    def get_masks(self):
        for lst_idx, lst in enumerate(self.sequence_lists):
            for mask in lst.get_masks():
                yield lst_idx, mask

    def _register_holder(self, header, seqstart, filepos):
        if header is not None:
            seq_holder = SequenceHolder(header, seqstart, filepos-1, self)
            self.sequence_lists[-1].add_sequence_holder(seq_holder)

    def init_content(self):
        self.index_sequences()
        self.sequence_lists[0].init_masks()

    def index_sequences(self):
        """ Read the positions of the sequences in the file.
        """
        self.sequence_lists = [SequenceList()]

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
        copy = FileManager(self.original_name)
        
        copy.sequence_lists = [x.copy() for x in self.sequence_lists]
        copy.verbose = self.verbose
        return copy


    def extract(self, dest_file):
        """
        Extract sequences subparts from the origin file to generate the current file

            Parameters:
                dest_file (string): Path to the file to generate
        """
        
        with open(dest_file, "w") as extract, open(self.filename, "rb") as origin:
            # Writes each sub-sequence of the file
            for seq_lst in self.sequence_lists:
                for seq_holder in seq_lst:
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

    def num_masks(self):
        return sum(x.num_masks() for x in self.input_sequences.values())

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

            
