# Concurrency Control
Rigorous 2 PL concurrency control protocol with cautious waiting and wait-die for dealing with deadlock

We have implemented Cautious wait and Wait die concurreny control protocol.
Both protocols have separate application file.

# Programming Language:

Python: 3.8

# How to execute:

1. Copy input text file in the same location as program file.
2. Run .py file: wait_die.py <input_file>.txt <output_file>.txt or cautious_wait.py  <input_file>.txt <output_file>.txt
3. Command prompt run command
   python3 wait_die.py <input_file>.txt <output_file>.txt 
   or
   python3 cautious_wait.py <input_file>.txt <output_file>.txt
4. Output file with given name will be created. Prior creation of output file is not needed
E.g. python3 wait_die.py input1.txt output1_wd.txt
E.g. python3 cautious_wait.py input1.txt output1_cw.txt

# References
1. https://newbedev.com/what-is-the-difference-between-wait-die-and-wound-wait-deadlock-prevention-algorithms
2. https://github.com/chaitanya-sardesai/2-phase-lock-rigorous/blob/master/src/rigorous_2pl.py
3. https://github.com/sksq96/conservative-two-phase-locking
