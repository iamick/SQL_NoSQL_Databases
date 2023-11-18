import pandas as pd
import sqlite3
import datetime
from .utils import highlight_cells_above, highlight_cells_below

con = sqlite3.connect("synthetic_claims.db")
cur = con.cursor()

def check_nulls(df):
    """
    This function checks for nulls in a pandas df and outputs a conditionally formatted df of null counts
    input: pandas df
    output: pandas df of null counts
    """
    
    null_df = df.isna().sum().to_frame()
    null_df.columns = ['# Nulls']
    return null_df.style.applymap(highlight_cells_above, threshold=0)

def check_corr(df, threshold):
    """
    This function checks for correlation of variables in a pandas df and outputs a conditionally formatted df of pairwise correlation of columns
    input: pandas df
    output: pandas df of pairwise correlation of columns
    """
    
    return df.corr().style.applymap(highlight_cells_above, threshold=threshold)

def check_var(df, threshold):
    """
    This function checks the variance of variables in a pandas df and outputs a conditionally formatted df of column variance
    input: pandas df
    output: pandas df of column variance
    """
    
    var_df = df.var().to_frame()
    var_df.columns = ['Variance']
    
    return var_df.style.applymap(highlight_cells_below, threshold=threshold)

def binarize_visits(row):
    """
    This function converts the ER_Visits column to a binary variable (0 = 0, > 0 = 1)
    input: pandas df
    output: pandas df 
    """
    
    ER_num = row['ER_Visits']
    if ER_num > 0:
        ER_bin = 1
    else:
        ER_bin = 0
        
    return ER_bin

    
