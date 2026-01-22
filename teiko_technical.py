import numpy as np
import pandas as pd

'''
Load CSV data from file given by filename into a pandas dataframe
'''
def load_data(filename):
    f = open(filename, "r")
    data = pd.read_csv(f)
    return data

'''
Given a dataframe of sample data, output a summary table of basic statistics
'''
def overview(df):
    pass


data = load_data("cell-count.csv")
print(data.head)