#best runs on the whole dataset 2GB

from mrjob.job import MRJob
from Clustering import MRJobKMeans
import sys, os, random
import os.path
import shutil
from math import sqrt
import time
from operator import itemgetter, attrgetter

InitialCentroids = "centroids.txt"
TempFile = "centroids_temp.txt"
FinalClusters = "final_clusters.txt"
convergence_delta = 0.01
k_delta = 10

def generate_init_centroids(num):
    centroids = []
    for i in range(0, num):
        centroid = (str(i) + "," + str(random.uniform(41.6, 42.05)) + "," + str(random.uniform(-87.5, -87.95)))
        centroids.append(centroid)
    return centroids

def write_to_file(centroids, file):
    with open(file, 'w') as f:
        centroids = sorted(centroids, key=itemgetter(0))
        for centroid in centroids:
            k, cx, cy = centroid.split(",")
            f.write("%s,%s\n" % (cx, cy))

def read_from_file(file):
    centroids = []
    with open(file, 'r') as f:
        for line in f:
            if line:
                cords = line.split(",")
                x, y = cords
                centroids.append([float(x), float(y)])
    return centroids


def get_random_coords_in_region(id):
    centroid = (str(id) + "," + str(random.uniform(41.6, 42.05)) + "," + str(random.uniform(-87.5, -87.95)))
    return centroid


def missing_elements(L, k_delta):
    start, end = 0, k_delta
    return sorted(set(range(start, end)).difference(L))


def kmeans_check(centroids, k_delta):
    centroids = sorted(centroids, key=itemgetter(0))
    exist = []
    missing_centroids = []
    for centroid in centroids:
        k, cx, cy = centroid.split(",")
        exist.append(int(k))
    missing_centroids = missing_elements(exist, k_delta)

    if missing_centroids == []:
        return centroids
    else:
        for id in missing_centroids:
            centroid = get_random_coords_in_region(id)
            print("Regenerated centroid:", centroid)
            centroids.append(centroid)
            centroids = sorted(centroids, key=itemgetter(0))
        return centroids

def eucl_dist(point, centroid):
    return sqrt(pow(centroid[0] - point[0], 2) + pow(centroid[1] - point[1], 2))

def get_job_centroids(job, runner):
    centroids = []
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        centroids.append(value)
    return centroids

def difference(c1, c2):
    max_difrnc = 0.0
    for i in range(len(c1)):
        dist = eucl_dist(c1[i], c2[i])
        if dist > max_difrnc:
            max_difrnc = dist
    return max_difrnc


if __name__ == '__main__':
    start_time = time.time()

    args = sys.argv[1:]
    print(args)

    generated_centroids = generate_init_centroids(k_delta)
    write_to_file(generated_centroids, InitialCentroids)
    shutil.copy(InitialCentroids, TempFile)
    old_centroids = read_from_file(InitialCentroids)
    i = 1
    while True:
        print ("Step #%i" % i)
        mr_job = MRJobKMeans(args=args + ['--centroids=' + TempFile])
        print("Created MRJob #%i" % i)
        #mr_job.set_up_logging(stream=sys.stdout)
        with mr_job.make_runner() as runner:
            print("Created runner on MRJob #%i" % i)
            runner.run()
            print("Called run on runner #%i" % i)
            centroids = get_job_centroids(mr_job, runner)
            print("Calculated centroids #%i" % i)
            centroids2 = kmeans_check(centroids, k_delta)
            print("Kmeans check finished #%i" % i)
            write_to_file(centroids2, TempFile)
            print("Wrote centroids to file - TempFile #%i" % i)
        new_centroids = read_from_file(TempFile)

        max_dif = difference(new_centroids, old_centroids)
        print("Maximum difference: ", max_dif)
        if max_dif < convergence_delta:
            print("Final Clusters:")
            j = 1
            for c in new_centroids:
                print(j, c[0], c[1])
                j += 1
            elapsed_time = time.time() - start_time
            write_to_file(centroids2, FinalClusters)
            os.remove(TempFile)
            os.remove(InitialCentroids)
            print("Running time: %i - seconds" % elapsed_time)
            print("Amount of iterations: %i" % i)
            print("Average time per iteration: %f" % float(elapsed_time / i))
            break
        else:
            old_centroids = new_centroids
            i += 1

