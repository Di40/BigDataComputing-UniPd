from collections import defaultdict
from pyspark import SparkContext, SparkConf
import os
import sys
import time
from statistics import median
import random

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    # We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)
    # Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    # Create a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:
        u, v = edge
        node_colors[u] = ((rand_a * u + rand_b) % p) % num_colors
        node_colors[v] = ((rand_a * v + rand_b) % p) % num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors == triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def hash_function(u, params):
    a, b, p, C = params
    return ((a * u + b) % p) % C

def MR_ApproxTCwithNodeColors(rdd, C=1):
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    params = [a, b, p, C]

    def mapEdgesUsingColor(edge):
        hc_u = hash_function(edge[0], params)
        hc_v = hash_function(edge[1], params)
        if hc_u == hc_v:
            return [(hc_u, edge)]
        return []

    return C ** 2 * rdd.flatMap(lambda x: mapEdgesUsingColor(x)) \
                       .groupByKey() \
                       .map(lambda x: CountTriangles(x[1])) \
                       .reduce(lambda x, y: x + y)

def MR_ExactTC(rdd, C=1):
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)
    params = [a, b, p, C]

    return rdd.flatMap(lambda e: [(tuple(sorted((hash_function(e[0], params), hash_function(e[1], params), i))), e)
                                  for i in range(C)]).groupByKey()\
              .map(lambda x: countTriangles2(x[0], x[1], *params)) \
              .reduce(lambda x, y: x + y)

def main():
    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCounting_CloudVeneto_HW2')
    conf.set("spark.locality.wait", "0s")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # INPUT READING
    C = int(sys.argv[1])
    R = int(sys.argv[2])
    F = int(sys.argv[3])
    file_path = sys.argv[4]

    rawData = sc.textFile(file_path, minPartitions=C). \
        map(lambda x: tuple(map(int, x.strip().split(",")))). \
        repartition(numPartitions=32). \
        cache()

    file_name = os.path.basename(file_path)
    num_edges = rawData.count()
    num_executors = sc.getConf().get("spark.executor.instances")

    if not F:
        t_final_aprox_list = []
        running_time_aprox_list = []
        for _ in range(R):
            start_time = time.time()
            t_final = MR_ApproxTCwithNodeColors(rawData, C)
            end_time = time.time()
            t_final_aprox_list.append(t_final)
            print(end_time, '-', start_time, '=', end_time - start_time)
            running_time_aprox_list.append(end_time - start_time)
        print(running_time_aprox_list)
        runningTime_MR_ApproxTCwithNodeColors = int(1000 * (sum(running_time_aprox_list) / R))
        median_MR_ApproxTCwithNodeColors = median(t_final_aprox_list)
        print('OUTPUT with parameters: with {} executors, {} colors, {} repetitions, flag {}, file  {}\n\n\
              Dataset = {}\n\
              Number of Edges = {}\n\
              Number of Colors = {}\n\
              Number of Repetitions = {}\n\
              Approximation algorithm with node coloring\n\
              - Number of triangles = {}\n\
              - Running time (average over {} runs) = {} ms'.format(num_executors, C, R, F, file_name,
                                                                    file_name, num_edges, C, R,
                                                                    median_MR_ApproxTCwithNodeColors, R, 
                                                                    runningTime_MR_ApproxTCwithNodeColors))
    else:
        t_final_exact_list = []
        running_time_exact_list = []
        for _ in range(R):
            start_time = time.time()
            t_final = MR_ExactTC(rawData, C)
            end_time = time.time()
            t_final_exact_list.append(t_final)
            running_time_exact_list.append(end_time - start_time)
        runningTime_MR_ExactTC = int(1000 * (sum(running_time_exact_list) / R))
        last_MR_ExactTC = t_final_exact_list[-1]
        print('OUTPUT with parameters:  with {} executors, {} colors, {} repetitions, flag {}, file  {}\n\n\
              Dataset = {}\n\
              Number of Edges = {}\n\
              Number of Colors = {}\n\
              Number of Repetitions = {}\n\
              Exact algorithm with node coloring\n\
              - Number of triangles = {}\n\
              - Running time (average over {} runs) = {} ms'.format(num_executors, C, R, F, file_name,
                                                                    file_name, num_edges, C, R, last_MR_ExactTC, R,
                                                                    runningTime_MR_ExactTC))


if __name__ == '__main__':
    main()
