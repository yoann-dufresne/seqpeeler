#!/bin/python3

# Cmd line: `python3 triplets_multifasta.py file1.fa file2.fa [... fasta list]`
# Program raise an exception on triple repetition of 2 nucleotides


from sys import argv


def parse():
	py_idx = 0
	for i, name in enumerate(argv):
		if name.endswith("triplets_multifasta.py"):
			py_idx = i
			break

	return argv[py_idx+1:]


def read_file(file):
	with open(file) as fp:
		window = ['.'] * 6
		win_idx = 0

		for line in fp.readlines():
			# skip header
			if line[0] == '>':
				window = ['.'] * 6
				win_idx = 0
				continue

			line = line.strip()
			# read nucleotide by nucleotide
			for nucl in line:
				# add the nucl to the window
				window[win_idx % 6] = nucl
				win_idx += 1

				# Test the triple doublet
				if (window[0] == window[2]) and (window[0] == window[4]) and (window[1] == window[3]) and (window[1] == window[5]):
					raise Exception("Triple doublet")


if __name__ == "__main__":
	files = parse()

	for file in files:
		read_file(file)

	print("Program ended successfully")
