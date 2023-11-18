import pandas as pd
import sqlite3

con = sqlite3.connect("synthetic_claims.db")
cur = con.cursor()

def unique_ids(table_name):
    """
    This function counts the number of unique Patient Ids, total Patient Ids, and duplicate Patient Ids in a given table
    input: sql table
    output: counts as printed str
    """
    
    sql = f"""
    SELECT COUNT(DISTINCT `Patient Id`), 
           COUNT(`Patient Id`), 
           COUNT(`Patient Id`) - COUNT(DISTINCT `Patient Id`)
    FROM {table_name}
    """

    cur.execute(sql)
    
    results = cur.fetchone()
    formatted_results = f"""
    {table_name} Table Duplicate Ids
    -------------------------------
    Unique Patient Ids: {results[0]}
    Total Patient Ids: {results[1]}
    Duplicate Patient Ids: {results[2]}
    """
    
    return(print(formatted_results))

def date_range(table_name):
    """
    This function finds the first and last dates in a given table
    input: sql table
    output: dates as printed str
    """
    
    sql = f"""
    SELECT MIN(Date) as Start_Date, 
           MAX(Date) as End_Date
    FROM {table_name}
    """
    
    cur.execute(sql)
    
    results = cur.fetchone()
    formatted_results = f"""
    {table_name} Table Date Range
    -------------------------------
    Start Date: {results[0]}
    End Date: {results[1]}
    """
    
    return(print(formatted_results))

def value_counts(table_name):
    """
    This function returns the value counts for each column in a given table
    input: sql table
    output: value counts as printed str
    """
    
    cur.execute(f"PRAGMA TABLE_INFO({table_name})")
    column_list = []
    for i in cur.fetchall():
        column_list.append(i[1])
        
    for col in column_list[2:]:
        sql = f"""
        SELECT `{col}`, 
               COUNT(`{col}`),
               (SELECT COUNT(*) FROM {table_name})
        FROM {table_name}
        GROUP BY `{col}`
        """

        cur.execute(sql)

        results = cur.fetchall()
        formatted_results = f"""
        {col} Value Counts
        -------------------------------
        Percent 0: {100 * results[0][1]/ results[0][2]:.2f}%
        """

        print(formatted_results)
        
    return(None)


def highlight_cells_above(val, threshold, color='red'):
    """
    This function returns a conditionally formatted pandas df that highlights correlation values > 0.7
    input: Pandas df
    output: Pandas df
    """
    color = 'red' if val > threshold else ''
    return 'background-color: {}'.format(color)

def highlight_cells_below(val, threshold, color='red'):
    """
    This function returns a conditionally formatted pandas df that highlights correlation values > 0.7
    input: Pandas df
    output: Pandas df
    """
    color = 'red' if val < threshold else ''
    return 'background-color: {}'.format(color)