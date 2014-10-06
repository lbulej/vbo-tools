vbo-tools
=========

Simple utilities for RaceLogic VBO files.


csv2vbo.py
----------

Converts .csv files produced by various datalogging software
.vbo files produced by RaceLogic's dataloggers and understood
by the CircuitTools software. The script requires Python 3 and
currently supports the following .csv variants:

  - RaceChrono
  - G-Tech Fanatic
  - TrackMaster

    TrackMaster .csv can be obtained by manual export
    from a .xls file (only the overview and lap sheets).

csv2vbo.py expects a .csv file on standard input and writes
a .vbo file to standard output. It will either detect the variant
of the input .csv file automatically or, failing that, exit with
an error. For example, to convert "log.csv" into "log.vbo", the 
following command needs to be issued:

$ ./csv2vbo.py < log.csv > log.out

