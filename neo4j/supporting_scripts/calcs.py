import neo4j
import math
import numpy as np
import pandas as pd
import psycopg2

from geographiclib.geodesic import Geodesic

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

def my_neo4j_shortest_path_time(from_station, to_station):
    """
    Given a from station and to station, run and print the shortest path total time
    """
    
    query = "CALL gds.graph.drop('ds_graph', false)"
    session.run(query)

    query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: 'weight'})"
    session.run(query)

    query = """

    MATCH (source:Station {name: $source}), (target:Station {name: $target})
    CALL gds.shortestPath.dijkstra.stream(
        'ds_graph', 
        { sourceNode: source, 
          targetNode: target, 
          relationshipWeightProperty: 'weight'
        }
    )
    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
    RETURN
        gds.util.asNode(sourceNode).name AS from,
        gds.util.asNode(targetNode).name AS to,
        totalCost,
        [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodes,
        costs
    ORDER BY index

    """

    result = session.run(query, source=from_station, target=to_station)
    
    for r in result:
        
        total_cost = int(r['totalCost'])
        from_station = r['from']
        to_station = r['to']
        
        print("\n-------------------------------------------------------")
        print(f"   {from_station}, {to_station}:")
        print("   Total Travel Time: ", round(total_cost / 60.0,1), " mins")
        print("-------------------------------------------------------")
        

def my_neo4j_shortest_path(from_station, to_station):
    """
    Given a from station and to station, run and print the shortest path
    """
    
    query = "CALL gds.graph.drop('ds_graph', false)"
    session.run(query)

    query = "CALL gds.graph.project('ds_graph', 'Station', 'LINK', {relationshipProperties: 'weight'})"
    session.run(query)

    query = """

    MATCH (source:Station {name: $source}), (target:Station {name: $target})
    CALL gds.shortestPath.dijkstra.stream(
        'ds_graph', 
        { sourceNode: source, 
          targetNode: target, 
          relationshipWeightProperty: 'weight'
        }
    )
    YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
    RETURN
        gds.util.asNode(sourceNode).name AS from,
        gds.util.asNode(targetNode).name AS to,
        totalCost,
        [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodes,
        costs
    ORDER BY index

    """

    result = session.run(query, source=from_station, target=to_station)
    
    for r in result:
        
        total_cost = int(r['totalCost'])
        
        print("\n--------------------------------")
        print("   Total Cost: ", total_cost)
        print("   Minutes: ", round(total_cost / 60.0,1))
        print("--------------------------------")
        
        nodes = r['nodes']
        costs = r['costs']
        
        i = 0
        previous = 0
        
        for n in nodes:
            
            print(n + ", " + str(int(costs[i]) - previous)  + ", " + str(int(costs[i])))
            
            previous = int(costs[i])
            i += 1

def my_calculate_box(point, miles):
    """
    Given a point and miles, calculate the box in form left, right, top, bottom
    """
    
    geod = Geodesic.WGS84

    kilometers = miles * 1.60934
    meters = kilometers * 1000

    g = geod.Direct(point[0], point[1], 270, meters)
    left = (g['lat2'], g['lon2'])

    g = geod.Direct(point[0], point[1], 90, meters)
    right = (g['lat2'], g['lon2'])

    g = geod.Direct(point[0], point[1], 0, meters)
    top = (g['lat2'], g['lon2'])

    g = geod.Direct(point[0], point[1], 180, meters)
    bottom = (g['lat2'], g['lon2'])
    
    return(left, right, top, bottom)

def my_station_get_zips(station, miles):
    """
    Given a station, pull all zip codes with miles distance, print them, sum the population
    """
    
    connection.rollback()
    
    query = "select latitude, longitude from stations "
    query += "where station = '" + station + "'"
    
    cursor.execute(query)
    
    connection.rollback()
    
    rows = cursor.fetchall()
    
    for row in rows:
        latitude = row[0]
        longitude = row[1]
        
    point = (latitude, longitude)
        
    (left, right, top, bottom) = my_calculate_box(point, miles)
    
    query = "select zip, population from zip_codes "
    query += " where latitude >= " + str(bottom[0])
    query += " and latitude <= " + str(top [0])
    query += " and longitude >= " + str(left[1])
    query += " and longitude <= " + str(right[1])
    query += " order by 1 "

    cursor.execute(query)
    
    connection.rollback()
    
    rows = cursor.fetchall()
    
    print("\n-------------------------------------------------------------------------------")
    print("  Zip Codes within " + str(miles) + " mile(s) of " + station + " BART Station")
    print("-------------------------------------------------------------------------------\n")
    
    total_population = 0
    
    for row in rows:
        zip = row[0]
        population = row[1]
        print("     zip:", zip, "  population: ", f'{population:10,}')
        total_population += population
        
    
    print("\n-------------------------------------------------------------------------------")
    print("  Total Population: ", f'{total_population:10,}')
    print("-------------------------------------------------------------------------------")
    
def my_station_zips_count(station, miles):
    """
    Given a station, pull all zip codes with miles distance, return sum of the populations
    """
    
    connection.rollback()
    
    query = "select latitude, longitude from stations "
    query += "where station = '" + station + "'"
    
    cursor.execute(query)
    
    connection.rollback()
    
    rows = cursor.fetchall()
    
    total_population = 0
    
    for row in rows:
        latitude = row[0]
        longitude = row[1]
        
    point = (latitude, longitude)
        
    (left, right, top, bottom) = my_calculate_box(point, miles)
    
    query = "select zip, population from zip_codes "
    query += " where latitude >= " + str(bottom[0])
    query += " and latitude <= " + str(top [0])
    query += " and longitude >= " + str(left[1])
    query += " and longitude <= " + str(right[1])
    query += " order by 1 "

    cursor.execute(query)
    
    connection.rollback()
    
    rows = cursor.fetchall()
    
    for row in rows:
        zip = row[0]
        population = row[1]
        total_population += population
        
    return total_population
    