### This adds the path location where we have the src folders stored
import sys, os, argparse, re

### imports ###
from plotly import __version__
import pandas as pd
import shutil

from src.lib import sc_tools

# supress sklearn warnings
from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

pd.options.mode.chained_assignment = None 

Base_Path = '/mnt/apps/risk_topic_evolution/code'                            # This is the path to code dir
Data_Path = '/mnt/data/risk_topic_evolution'            # This is the path to the data

Raw_Path       = os.path.join(Base_Path, "data", "raw")
Processed_Path = os.path.join(Data_Path, "processed")
Twitter_Path   = os.path.join(Data_Path, "Twitter_data")
Reddit_Path    = os.path.join(Data_Path, "Reddit_data")
Parler_Path    = os.path.join(Data_Path, "Parler_data")
Avaliable_Path_Dict = {'twitter': Twitter_Path,
                        'reddit' : Reddit_Path,
                        'parler' : Parler_Path}

sys.path.insert(0, Base_Path)
sys.path.insert(0, os.path.join(Base_Path, "src"))


Make_Changes = True
# Make_Changes = False

in_dir = os.path.join(Parler_Path, '2formattemp')
outdir = os.path.join(Parler_Path, '2format')
print(in_dir + " --->>>--- " + outdir)

def main ():
    '''
    The parler dataset is non chronological, 
    Multiple raw data file can contain the same hour
    To run the 2_process_data in parallel the output files cannot be the same
    This means there needs to be multiple 2format folders in the format '/2[a-z]format'
    This block of code will merge multiple instances of a file into one. 
    '''
    count = 0
    for dirName, subdirList, fileList in os.walk(in_dir):
        reldir = dirName.replace(in_dir, '')
        reldir2 = re.sub('/2[a-z]format', '', reldir)
        
        for fname in fileList:
            if os.path.splitext(fname)[1] == '.pkl':
                count += 1
                file_path_in = os.path.join(dirName, fname)
                dir_out2 = outdir + reldir2
                file_path_out = os.path.join(dir_out2, fname)

                print(str(count) + " - " + file_path_in + " -- " + file_path_out + " -- " + dir_out2)


                if Make_Changes:
                    os.makedirs(dir_out2, exist_ok=True)                         # Make folders that do not exist
                    if not os.path.isfile(file_path_out):
                        shutil.copyfile(file_path_in, file_path_out)

                    else: 
                        sc_tools.merge_dataset_save(file_path_out, file_path_in, file_path_out)

    print("Files: " + str(count))


if __name__ == '__main__':
    main()
