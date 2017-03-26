#!/bin/env python
## /prog/python/2.7.9/bin/python    # started using in v .6 (2016.0304)

# this file perform a conversion of accession to hash (text to number)
# for the files used to populate the taxonomy database 
# example run:
# python acc2hash.py  -dd prot.accession2taxid                 prot.acchash.tsv

from __future__ import print_function
# needed to print to file.  ref: http://stackoverflow.com/questions/12592544/typeerror-expected-a-character-buffer-object
import argparse 
import sys


def process_cli() :
        # https://docs.python.org/2/howto/argparse.html#id1
        parser = argparse.ArgumentParser( description='convert accession and acc.ver into hashes, ready for load into db')
        # https://docs.python.org/2/library/argparse.html#nargs
        parser.add_argument("infile",  help="name of  input file to process (def STDIN)",  nargs='?', type=argparse.FileType('r'), default=sys.stdin )
        parser.add_argument('outfile', help="name of output file            (def STDOUT)", nargs='?', type=argparse.FileType('w'), default=sys.stdout)
        # the above will open the file handle already, so output file will be touched/zeroed even if nothing else is printed to it.
        parser.add_argument('-s', '--separator', '--ifs', dest='IFS',  help="the column separator character (defualt TAB)", default='	' ) 
        parser.add_argument('-d', '--debuglevel', help="Debug mode. Up to -ddd useful for troubleshooting input file parsing. -ddddd intended for coder. ", action="count", default=0)
        parser.add_argument('--version', action='version', version='%(prog)s 0.7.3  For help, email tin.ho@novartis.com')
        args = parser.parse_args()
        return args
# end process_cli() 

# expected input:
# accession       accession.version       taxid   gi
# P15711  P15711.1        5875    112670
# P18646  P18646.1        3917    112671
# example output (into file):
# -8588756791137006371    2941236429707352068     5875
# -3588740791115006317    6798284115561641610     3917
def run_converter( args ) :
        expectedColumns = 4             # for sanity check of input file

        infile  = args.infile
        outfile = args.outfile
        if( args.debuglevel >= 2 ) :
            print( "<!--input  file is %s-->"   % infile  )
            print( "<!--output file is %s-->"   % args.outfile )
        if( args.debuglevel >= 1 ) :
            print( "<!--separator to use is '%s'-->" % args.IFS )

        for line in infile:
            line = line.rstrip("\n\r")
            lineList = line.split( args.IFS )

            if( len(lineList) < expectedColumns ) :
                print( "Not enough columns.  Skipping line.  No accession num found for input line of '%s'" % line )
                #dbg( 3, "Line split into %s words" % len (lineList) )
                continue

            (accession, accession_version, tax_id, gi) = lineList

            ## need to strip header row that reads "accession   accession.version   taxid    gi"     (tab separated)
            if( accession == "accession" ):
                    continue
            ha  = hash(accession)
            hav = hash(accession_version)
            # gi is dropped
            # print to output file
            print( "%d\t%d\t%s" % (ha, hav, tax_id), file = outfile )
            # could do some regexp to validate input, but since it is ncbi dump, should be fairly standard, skipping for now
            #if( re.search( '^[0-9]+$', gi ) ) :
                # extract string does indeed has a all-numeric format, reasonable as gi, returns it
                #dbg( 2, "successfully extracted gi [%12s] from input line of '%s'" % (gi, line) )
        infile.close()
        outfile.close()


def main():
        args = process_cli()
        run_converter( args ) 
# main()-end


### end of all fn definition, begin of main program flow.
main()

