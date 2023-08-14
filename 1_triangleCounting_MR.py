import sys
import os
import time
import random
from pyspark import SparkContext, SparkConf
from statistics import median
from collections import defaultdict

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


def main():
    assert len(sys.argv) == 4, "Usage: python G095HW1_old.py <C> <R> <file_path>"

    # SPARK SETUP
    conf = SparkConf().setAppName('TriangleCounting_HW1')
    sc   = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    # INPUT READING
    C = sys.argv[1]
    assert C.isdigit(), "C must be an integer"
    C = int(C)
    R = sys.argv[2]
    assert R.isdigit(), "R must be an integer"
    R = int(R)
    file_path = sys.argv[3]
    assert os.path.isfile(file_path), "File or folder not found"

    rawData = sc.textFile(file_path, minPartitions=C).\
        map(lambda x: tuple(map(int, x.strip().split(",")))). \
        repartition(numPartitions=C). \
        cache()

    file_name = os.path.basename(file_path)
    num_edges = rawData.count()

    # MR_ApproxTCwithNodeColors
    t_final_list = []
    running_time_list = []
    for _ in range(R):
        start_time = time.time()
        t_final    = MR_ApproxTCwithNodeColors(rawData, C)
        end_time   = time.time()
        t_final_list.append(t_final)
        running_time_list.append(end_time - start_time)
    runningTime_MR_ApproxTCwithNodeColors = int(1000 * (sum(running_time_list) / R))
    median_MR_ApproxTCwithNodeColors      = median(t_final_list)

    # MR_ApproxTCwithSparkPartitions
    start_time = time.time()
    t_final_MR_ApproxTCwithSparkPartitions = MR_ApproxTCwithSparkPartitions(rawData, C)
    end_time   = time.time()
    runningTime_MR_ApproxTCwithSparkPartitions = int(1000 * (end_time - start_time))

    # Printing
    print(f'Dataset = {file_name}\n\
    Number of Edges = {num_edges}\n\
    Number of Colors = {C}\n\
    Number of Repetitions = {R}\n\
    Approximation through node coloring\n\
    - Number of triangles (median over {R} runs) = {median_MR_ApproxTCwithNodeColors}\n\
    - Running time (average over {R} runs) = {runningTime_MR_ApproxTCwithNodeColors} ms\n\
    Approximation through Spark partitions\n\
    - Number of triangles = {t_final_MR_ApproxTCwithSparkPartitions}\n\
    - Running time = {runningTime_MR_ApproxTCwithSparkPartitions} ms')

def MR_ApproxTCwithNodeColors(rdd, C=1):
    p = 8191
    a = random.randint(1, p - 1)
    b = random.randint(0, p - 1)

    def hash_function(u):
        return ((a * u + b) % p) % C

    def mapEdgesUsingColor(edge):
        hc_u = hash_function(edge[0])
        hc_v = hash_function(edge[1])
        if hc_u == hc_v:
            return [(hc_u, edge)]
        return []

    return C ** 2 * rdd.flatMap(lambda x: mapEdgesUsingColor(x)) \
        .groupByKey() \
        .map(lambda x: CountTriangles(x[1])) \
        .reduce(lambda x, y: x + y)

def MR_ApproxTCwithSparkPartitions(rdd, C=1):
    return C**2 * (rdd.mapPartitions(lambda x: [CountTriangles(x)]).
                   reduce(lambda x, y: x + y))


if __name__ == "__main__":
    main()
