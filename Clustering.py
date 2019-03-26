from mrjob.job import MRJob
import mrjob
from math import sqrt


class MRJobKMeans(MRJob):
    SORT_VALUES = True  #Set this to True if you would like reducers to receive the values associated with any key in sorted order

    def configure_options(self):
        super(MRJobKMeans, self).configure_options()
        self.add_file_option('--centroids')

    def get_nearest_cluster(self, point, centroids):
        nearest_cluster_id = None
        nearest_distance = 10000000
        for i in range(len(centroids)):
            dist = self.eucl_dist(point, centroids[i])
            if dist < nearest_distance:
                nearest_cluster_id = i
                nearest_distance = dist
        return nearest_cluster_id

    def loadfile(self):
        centroids = []
        with open(self.options.centroids, 'r') as f:
            for line in f:
                lat, long = line.split(',')
                centroids.append([float(lat), float(long)])
        return centroids

    def eucl_dist(self, point, centroid):
        return sqrt(pow(centroid[0] - point[0], 2) + pow(centroid[1] - point[1], 2))

    def mapper(self, _, value):
        centroids = self.loadfile()
        line = value.replace(', ', '').strip()
        words = line.split(",")

        try:
            NR, ID, CaseNumber, Date, Block, IUCR, PrimaryType, Description, LocationDescription, Arrest, Domestic, Beat, District, Ward, CommunityArea, FBICode, XCoordinate, YCoordinate, Year, UpdatedOn, Latitude, Longitude, Location = words
            point = [float(Latitude), float(Longitude)]

            for i in range(len(centroids)):
                dist = self.eucl_dist(point, centroids[i])
                if dist < min_dist:
                    min_dist = dist
                    centroid = i

            #centroid = self.get_nearest_cluster(point, centroids)

            yield centroid, point
            # 7, (41.232,-87.433)
            # 2, (41.534,-87.322)
            # 4, (41.669,-87.444), (41.322,-87.900)

        except ValueError:
            pass

    def combiner(self, cluster, points):
        count = 0
        sum_x = 0.0
        sum_y = 0.0
        for point in points:
            count += 1
            sum_x += point[0]
            sum_y += point[1]
        yield cluster, (sum_x / count, sum_y / count)

    def reducer(self, cluster, points):
        count = 0
        sum_x = 0.0
        sum_y = 0.0
        for point in points:
            count += 1
            sum_x += point[0]
            sum_y += point[1]
        yield None, (str(cluster) + "," + str(sum_x / count) + "," + str(sum_y / count))


if __name__ == '__main__':
    MRJobKMeans.run()