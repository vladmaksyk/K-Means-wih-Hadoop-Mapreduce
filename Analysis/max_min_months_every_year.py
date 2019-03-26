from mrjob.job import MRJob
from mrjob.step import MRStep

class MRRatingCounter(MRJob):
    SORT_VALUES = True
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_avg)
        ]

    def mapper(self, key, value):
        line = value.replace(', ', '').strip()
        words = line.split(",")

        try:
            NR, ID, CaseNumber, Date, Block, IUCR, PrimaryType, Description, LocationDescription, Arrest, Domestic, Beat, District, Ward, CommunityArea, FBICode, XCoordinate, YCoordinate, Year, UpdatedOn, Latitude, Longitude, Location = words
            Month = Date[:2]
            Date = Year + "/" + Month
            yield Date, 1

        except ValueError:
            pass

    def reducer(self, key, values):
        yield key[0:4], (key[5:], sum(values))

    def mapper_init(self):
        self.oldkey = None
        self.min = 10000000
        self.max = 0
        self.monthmin = None
        self.monthmax = None
        self.currentkey = None

    def mapper_avg(self, key, value):
        self.currentkey = key
        if self.oldkey and self.oldkey != self.currentkey:  # just after the key has changed yield the result of this key
            yield (self.oldkey, self.monthmax, self.max),(self.oldkey, self.monthmin, self.min)
            self.min = 1000
            self.max = 0
            self.monthmin = None
            self.monthmax = None

        if value[1] < self.min:
            self.min = value[1]
            self.monthmin = value[0]

        if value[1] > self.max:
            self.max = value[1]
            self.monthmax = value[0]

        self.oldkey = self.currentkey

if __name__ == '__main__':
    MRRatingCounter.run()
