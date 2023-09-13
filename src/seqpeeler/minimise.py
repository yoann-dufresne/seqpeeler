from pathlib import Path
from time import sleep, time, gmtime, strftime
from subprocess import Popen
from subprocess import PIPE
from shutil import rmtree

from seqpeeler.filemanager import ExperimentDirectory


NB_PROCESS_STARTED_SEQUENTIAL = 0
NB_PROCESS_ENDED_SEQUENTIAL = 0
NB_PROCESS_STARTED_PARALLEL = 0
NB_PROCESS_ENDED_PARALLEL = 0
NB_PROCESS_INTERRUPTED = 0
CHUNK_SIZE = 200_000_000 # char number, string of about 0.8 Go



class PopenExtended(Popen) :

    def __init__(self, args, bufsize=-1, executable=None, stdin=None, stdout=None, stderr=None, preexec_fn=None, close_fds=True, shell=False, cwd=None, env=None, universal_newlines=None, startupinfo=None, creationflags=0, restore_signals=True, start_new_session=False, pass_fds=(), *, encoding=None, errors=None, text=None, prioritised=True) :
        self.prioritised = prioritised # new attribute
        super().__init__(args=args, bufsize=bufsize, executable=executable, stdin=stdin, stdout=stdout, stderr=stderr, preexec_fn=preexec_fn, close_fds=close_fds, shell=shell, cwd=cwd, env=env, universal_newlines=universal_newlines, startupinfo=startupinfo, creationflags=creationflags, restore_signals=restore_signals, start_new_session=start_new_session, pass_fds=pass_fds, encoding=encoding, errors=errors, text=text)




def compare_output(acutal_output, desired_output) :
    rcode, stdout, stderr = acutal_output
    rcode2, stdout2, stderr2 = desired_output

    checkreturn = rcode2 is None or rcode2 == rcode
    checkstdout = stdout2 is None or stdout2 in stdout.read().decode()
    checkstderr = stderr2 is None or stderr2 in stderr.read().decode()
    r = checkreturn and checkstdout and checkstderr
    return r


    
# reduces the sequence, cutting first and last nucleotides
# cutting in half successively with an iterative binary search
# returns the new reduced sequence, WITHOUT ADDING IT TO THE SPECIE'S LIST OF SEQS
def strip_sequence(seq, sp, spbyfile, flag_begining, cmdargs) :
    (begin, end) = seq
    seq1 = (begin, end)

    imin = begin
    imax = end
    imid = (imin+imax) // 2
        
    while imid != imin and imid != imax :

        # get the most central quarter
        seq1 = (imid, end) if flag_begining else (begin, imid)
        sp.subseqs.append(seq1)
        dirname = prepare_dir(spbyfile, cmdargs)
        firstdirname = trigger_and_wait_processes(cmdargs, [dirname])
        rmtree(dirname)
        sp.subseqs.remove(seq1)

        # if the cut maintain the output, we keep cutting toward the center of the sequence
        if firstdirname is not None and firstdirname == dirname :
            if flag_begining :
                imin = imid
            else :
                imax = imid

        # else the cut doesn't maintain the output, so we keep cutting toward the exterior
        else :
            # keep the most external quarter
            seq1 = (imin, end) if flag_begining else (begin, imax)
            if flag_begining :
                imax = imid
            else :
                imin = imid
    
        imid = (imin+imax) // 2
    
    return seq1


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


# returns every reduced sequences of a file in a list of SpecieData
# Note: This function rely on side effects. This is reducing 1 file in a possible contect of multiple files
# TODO: Rewrite it without side effects
def reduce_one_file(file_manager, args) :
    # copy_iseqs = iseqs.copy()

    # for sp in copy_iseqs :
    #   # check if desired output is obtained whithout the sequence of the specie
    #   iseqs.remove(sp)
        
    #   dirname = prepare_dir(spbyfile, cmdargs)
    #   firstdirname = trigger_and_wait_processes(cmdargs, [dirname])
    #   rmtree(dirname)

    #   if firstdirname is None :
    #       # otherwise reduces the sequence
    #       iseqs.append(sp)

    for sp in iseqs :
        reduce_specie(sp, spbyfile, cmdargs)
        
    return iseqs


def reduce_file_set(file_managers, args) :
    
    if len(file_managers) == 1 :
        reduce_one_file(file_managers[0], args)
        return file_managers
    
    raise NotImplementedError("Multiple files not implemented")
    copy_spbyfile = file_managers.copy()

    for iseqs in copy_spbyfile :
        # check if desired output is obtained whithout the file
        file_managers.remove(iseqs)

        dirname = prepare_dir(file_managers, cmdargs)
        firstdirname = trigger_and_wait_processes(cmdargs, [dirname])
        rmtree(dirname)

        if firstdirname is None :
            # otherwise reduces the sequences of the file
            file_managers.append(iseqs)

    for iseqs in file_managers :
        reduce_one_file(iseqs, file_managers, cmdargs)

    return file_managers



def write_stats(duration, filepath) :
    NB_PROCESS_ENDED_PARALLEL = NB_PROCESS_STARTED_PARALLEL - NB_PROCESS_INTERRUPTED
    n = NB_PROCESS_STARTED_SEQUENTIAL + NB_PROCESS_STARTED_PARALLEL
    n2 = NB_PROCESS_ENDED_SEQUENTIAL + NB_PROCESS_ENDED_PARALLEL
    duration_str = strftime("%Hh %Mm %Ss", gmtime(duration))

    s = "Duration of execution: " + duration_str + "\n"
    s += "Number of processes started: " + str(n) + ", including\n" 
    s += "\t* " + str(NB_PROCESS_STARTED_SEQUENTIAL) + " in sequential\n"
    s += "\t* " + str(NB_PROCESS_STARTED_PARALLEL) + " in parallel\n"
    s += "Number of processes ended by themselves: " + str(n2) + ", including\n" 
    s += "\t* " + str(NB_PROCESS_ENDED_SEQUENTIAL) + " in sequential\n"
    s += "\t* " + str(NB_PROCESS_ENDED_PARALLEL) + " in parallel"

    f = open(filepath, 'w')
    f.write(s)
    f.close()




