import neo4j
import csv
import math
import numpy as np
import pandas as pd
import psycopg2

driver = neo4j.GraphDatabase.driver(uri="neo4j://neo4j:7687", auth=("neo4j","w205"))

session = driver.session(database="neo4j")

connection = psycopg2.connect(
    user = "postgres",
    password = "ucb",
    host = "postgres",
    port = "5432",
    database = "postgres"
)

cursor = connection.cursor()


def my_select_query_pandas(query, rollback_before_flag, rollback_after_flag):
    """
    Function to run a select query and return rows in a pandas dataframe
    """
    
    if rollback_before_flag:
        connection.rollback()
    
    df = pd.read_sql_query(query, connection)
    
    if rollback_after_flag:
        connection.rollback()
    
    for column in df:
    
        if df[column].dtype == "float64":

            fraction_flag = False

            for value in df[column].values:
                
                if not np.isnan(value):
                    if value - math.floor(value) != 0:
                        fraction_flag = True

            if not fraction_flag:
                df[column] = df[column].astype('Int64')
    
    return(df)
    
def my_neo4j_wipe_out_database():
    """
    Wipe out database by deleting all nodes and relationships
    """
    
    query = "match (node)-[relationship]->() delete node, relationship"
    session.run(query)
    
    query = "match (node) delete node"
    session.run(query)

def my_neo4j_run_query_pandas(query, **kwargs):
    """
    Run a query and return the results in a pandas dataframe
    """
    
    result = session.run(query, **kwargs)
    
    df = pd.DataFrame([r.values() for r in result], columns=result.keys())
    
    return df

def my_neo4j_number_nodes_relationships():
    """
    Print the number of nodes and relationships
    """
   
    
    query = """
        match (n) 
        return n.name as node_name, labels(n) as labels
        order by n.name
    """
    
    df = my_neo4j_run_query_pandas(query)
    
    number_nodes = df.shape[0]
    
    
    query = """
        match (n1)-[r]->(n2) 
        return n1.name as node_name_1, labels(n1) as node_1_labels, 
            type(r) as relationship_type, n2.name as node_name_2, labels(n2) as node_2_labels
        order by node_name_1, node_name_2
    """
    
    df = my_neo4j_run_query_pandas(query)
    
    number_relationships = df.shape[0]
    
    print("-------------------------")
    print("  Nodes:", number_nodes)
    print("  Relationships:", number_relationships)
    print("-------------------------")
    
def my_neo4j_relationships_df():
    """
    Return the number of relationships for each node in a pandas dataframe
    """
    
    query = """
        match (n1)-[r]->(n2) where n1.name starts with 'depart'
        return n1.name as node_name_1, labels(n1) as node_1_labels, 
            type(r) as relationship_type, n2.name as node_name_2, labels(n2) as node_2_labels
        order by node_name_1, node_name_2
    """
    
    return my_neo4j_run_query_pandas(query)
    

def my_neo4j_create_node(station_name):
    "create a node with label Station"
    
    query = """
    
    CREATE (:Station {name: $station_name})
    
    """
    
    session.run(query, station_name=station_name)
    
def my_neo4j_create_relationship_one_way(from_station, to_station, weight):
    "create a relationship one way between two stations with a weight"
    
    query = """
    
    MATCH (from:Station), 
          (to:Station)
    WHERE from.name = $from_station and to.name = $to_station
    CREATE (from)-[:LINK {weight: $weight}]->(to)
    
    """
    
    session.run(query, from_station=from_station, to_station=to_station, weight=weight)
    
def my_neo4j_create_relationship_two_way(from_station, to_station, weight):
    "create relationships two way between two stations with a weight"
    
    query = """
    
    MATCH (from:Station), 
          (to:Station)
    WHERE from.name = $from_station and to.name = $to_station
    CREATE (from)-[:LINK {weight: $weight}]->(to),
           (to)-[:LINK {weight: $weight}]->(from)
    
    """
    
    session.run(query, from_station=from_station, to_station=to_station, weight=weight)
    
