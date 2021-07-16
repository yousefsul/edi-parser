import glob
import shutil

from edi_parser import EdiParser

if __name__ == '__main__':
    edi_files = glob.glob(pathname='edi_files/*.*')
    for file in edi_files:
        edi_parser = EdiParser(file)
        edi_parser.create_837_index()
