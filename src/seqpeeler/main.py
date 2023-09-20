import argparse
from time import time
from pathlib import Path
from os import path, mkdir
from shutil import rmtree

from seqpeeler.minimise import Peeler
from seqpeeler.filemanager import FileManager



# returns the list of filenames (as string) in the file of files
def fof_to_list(fofname) :
    try :
        with open(fofname) as fof :
            filesnames = []

            for line in fof :
                filename = line.rstrip('\n')
                if not path.isfile(filename):
                    raise FileNotFoundError(f"File from fof not found: {filename}")
                filenames.append(path.abspath(filename))

        return filesnames

    except IOError :
        print("fof " + fofname + " not found.")
        raise

        

def parsing_files(filesnames) :
    """ Reads a list of files and return their file manager objects
    """
    files = []

    for filename in filesnames :
        manager = FileManager(filename)
        manager.index_sequences()
        files.append(manager)

    return files



def relative_to_absolute(cmd, paths_to_preserve):
    splitted_cmd = cmd.split(' ')

    for idx, word in enumerate(splitted_cmd):
        try:
            if path.exists(word) and word not in paths_to_preserve:
                splitted_cmd[idx] = path.abspath(word)
        except:
            continue

    return ' '.join(splitted_cmd)


# prepare the argument parser and parses the command line
# returns an argparse.Namespace object
def parse_args() :
    parser = argparse.ArgumentParser(prog="seqpeeler", description="seqpeeler is a software that helps the user to minimize fasta examples. It takes as input a command line to execute and a behaviour to track. Then it iteratively peel the sequences until the behaviour disappears.")

    # non positionnal arguments
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument('-l', '--fasta-list', help="List of paths to fasta files to peel", type=str, nargs='+', default=None)
    inputs.add_argument('-fof', '--file-of-files', help="Path to the file of files that contains the input fasta to peel.", type=str, default=None)
    parser.add_argument('-c', '--command-line', help="Command line to use for the peeling", type=str, required=True)
    

    triggers = parser.add_argument_group("triggers", "Triggers used to define the tracked behaviour. At least, one of them must be set")
    triggers.add_argument('-r', '--returncode', help="Software return code to track", default=None, type=int)
    triggers.add_argument('-u', '--stdout', help="Text on standard output to track", default=None)
    triggers.add_argument('-e', '--stderr', help="Text on standard error to track", default=None)
    
    parser.add_argument('-d', '--outdir', help="Directory where all the results will be written. If the directory already exists, it will be overwritten.", default='Results')
    parser.add_argument('-o', '--outfilenames', help="The list of the paths to the output files that will be generated by the command line. seqpeeler needs this list to avoid output collisions on multithread.", nargs='*', type=str, default=[])

    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()
    if args.returncode is None and args.stdout is None and args.stderr is None :
        parser.error("You have to specify at least one trigger")
    
    return args



def main():

    starttime = time()

    # set and get the arguments
    args = parse_args()

    # Creates the results directory
    if path.exists(args.outdir):
        rmtree(args.outdir)
    args.outdir = path.abspath(args.outdir)
    mkdir(args.outdir)

    # get the arguments
    desired_output = (args.returncode, args.stdout, args.stderr)

    # Parse input sequences
    seqfiles = args.fasta_list if args.fasta_list is not None else fof_to_list(args.file_of_files)
    # Translate from relative to absolute paths for input files that are not part of the reduction process
    args.command_line = relative_to_absolute(args.command_line, frozenset(seqfiles + args.outfilenames))

    if args.verbose :
        print(f" - Triggers: {desired_output}")
        print(f" - Will perform peeling on: {','.join(seqfiles)}")
        print(f" - Command used: {cmdline}")

    # parse the sequences of each file
    file_managers = parsing_files(seqfiles)
    
    # process the data
    peeler = Peeler(args)
    peeler.reduce_file_set(file_managers)
    
    duration = time() - starttime
