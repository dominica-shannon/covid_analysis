# Merge the datsets into a single dataset csv file - combined_csv.csv
import os
import glob
import pandas as pd

os.chdir("Final Dataset")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
combined_csv.to_csv("combined_csv.csv", index=False)
