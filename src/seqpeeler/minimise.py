from pathlib import Path
from time import sleep, time, gmtime, strftime
from subprocess import Popen
from subprocess import PIPE
from shutil import rmtree

from seqpeeler.filemanager import ExperimentDirectory, ExperimentContent
from seqpeeler.processes import SequenceJob, mainscheduler



def reduce_uniq_file(experiment, args) :
    """ Iteratively reduce the sequences size from a uniq file until reaching the minimal example.
            
            Parameters:
                experiment (ExperimentContent): An object containing all the file indirection for the experiment.
                args (NameTuple): parsed command line arguments
    """
    
    while tmpsubseqs : # while set not empty
        
        seq = tmpsubseqs.pop() # take an arbitrary sequence of the specie
        sp.subseqs.remove(seq)
        (begin, end) = seq

        middle = (seq[0] + seq[1]) // 2     
        seq1 = (begin, middle)
        seq2 = (middle, end)

        # prepare the files and directories needed to check if they pass the test
        sp.subseqs.append(seq1)
        dirname1 = prepare_dir(spbyfile, cmdargs)
        sp.subseqs.remove(seq1)     

        sp.subseqs.append(seq2)
        dirname2 = prepare_dir(spbyfile, cmdargs)

        sp.subseqs.append(seq1)
        dirname3 = prepare_dir(spbyfile, cmdargs)
        sp.subseqs.remove(seq1)
        sp.subseqs.remove(seq2)

        dirnames = list()
        priorities = list()
        if middle != end :
            dirnames.append(dirname1)
            priorities.append(True)
        if middle != begin :
            dirnames.append(dirname2)
            priorities.append(True)
        if middle != end and middle != begin :
            dirnames.append(dirname3)
            priorities.append(False)
        
        #print("nombre de proc simultannés : ", len(dirnames))
        
        firstdirname = trigger_and_wait_processes(cmdargs, dirnames, priorities)
        rmtree(dirname1)
        rmtree(dirname2)
        rmtree(dirname3)

        # TODO : utiliser des elif au lieu des continue ?
        # case where the target fragment is in the first half
        if firstdirname is not None and firstdirname == dirname1 :
            #print("case 1")
            sp.subseqs.append(seq1)
            tmpsubseqs.append(seq1)
            continue
        
        # case where the target fragment is in the second half
        if firstdirname is not None and firstdirname == dirname2 :
            #print("case 2")
            sp.subseqs.append(seq2)
            tmpsubseqs.append(seq2)
            continue
        
        # case where there are two co-factor sequences
        # so we cut the seq in half and add them to the set to be reduced
        # TODO : relancer le check_output pour trouver les co-factors qui ne sont pas de part et d'autre du milieu de la séquence
        if firstdirname is not None and firstdirname == dirname3 :
            #print("case 3")
            sp.subseqs.append(seq1)
            sp.subseqs.append(seq2)
            tmpsubseqs.append(seq1)
            tmpsubseqs.append(seq2)
            continue
        
        # case where the target sequence is on both sides of the cut
        # so we strip first and last unnecessary nucleotids
        #print("case 4")
        seq = strip_sequence(seq, sp, spbyfile, True, cmdargs)
        seq = strip_sequence(seq, sp, spbyfile, False, cmdargs)
        sp.subseqs.append(seq)
    
    return None


def iterative_reduction(args):
    for job in mainscheduler.terminated_list:
        


def reduce_file_set(file_managers, args) :
    
    if len(file_managers) != 1 :
        raise NotImplementedError("Multiple files not implemented")
    
    # Prepare the context of exec
    exp_content = ExperimentContent()
    exp_content.set_inputs(file_managers)
    exp_content.set_outputs(args.outfilenames)

    # Create the first job (entire files)
    job = SequenceJob(exp_content, args.command_line, args.outdir)
    mainscheduler.submit_job(job)

    while len(mainscheduler.waiting_list) > 0:
        mainscheduler.run()