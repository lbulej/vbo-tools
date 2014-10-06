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

$ ./csv2vbo.py < log.csv > log.vbo


The script does not have overly strict requirements on the input .csv
file. It has to contain a header row with column names, and from that
point onward, only data rows with the same number of columns as the
header row may follow. The data rows may contain duplicate header
rows -- this may result from concatenating multiple .csv files exported
from a spreadsheet. The duplicate header rows are filtered out, as well
as duplicate consecutive data rows.

All rows prior to the header row are output into the resulting VBO file
as comments. The script considers the first row with the maximal number of
columns to be the header row. This usually works even in presence of
non-data rows prior to the header row, as long as the number of columns
in the data is greater than the number of columns in the non-date rows
prior to the header.

If the timestamp of two consecutive data rows exceeds 0.1 s, the script
automatically creates intermediate data rows by interpolating between 
the two data rows, with 0.1 second increments to simulate a 10 Hz GPS.
This makes working with the data in CircuitTools more reasonable (the
software itself does not do interpolation), but it cannot supplement
the vastly more accurate output of a 10 Hz GPS.
