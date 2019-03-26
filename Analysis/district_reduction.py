from mrjob.job import MRJob
from mrjob.step import MRStep

class MRRatingCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper2)
        ]

    def mapper1(self, key, value):
        line = value.replace(', ', '').strip()
        words = line.split(",")

        try:
            (NR, ID, CaseNumber, Date, Block, IUCR, PrimaryType, Description, LocationDescription, Arrest, Domestic, Beat,District, Ward, CommunityArea, FBICode, XCoordinate, YCoordinate, YEAR, UpdatedOn, Latitude, Longitude,Location) = words
            yield (YEAR, PrimaryType, District), 1

        except ValueError:
            pass

    def reducer1(self, YearTypeDist, occurrences):
        yield YearTypeDist, sum(occurrences)

    def mapper_init(self):
        self.oldkey = None
        self.currentkey = None
        self.maxdif = 0
        self.Crimerecords = {}
        self.keytuple = ()
        self.first_time_change = False
        self.most_reduced_crime = None
        self.district = None

    def mapper2(self, key, value):
        self.keytuple = int(key[0]), key[1], key[2]
        self.currentkey = int(key[0])

        if self.oldkey and self.oldkey != self.currentkey and self.first_time_change and self.currentkey == 2017:
            yield (self.oldkey - 15, self.oldkey), (self.most_reduced_crime, self.district, self.maxdif)

        if self.oldkey and self.oldkey != self.currentkey and self.first_time_change == False:
            self.first_time_change = True

        if self.currentkey == 2016 and self.first_time_change == True:
            self.Crimerecords[self.keytuple] = value

            str1 = self.keytuple[0] - 15
            str2 = self.keytuple[1]
            str3 = self.keytuple[2]

            self.keytuple = str1, str2, str3

            if self.keytuple in self.Crimerecords:
                if self.Crimerecords[self.keytuple] - value > self.maxdif:
                    self.maxdif = self.Crimerecords[self.keytuple] - value
                    self.most_reduced_crime = self.keytuple[1]
                    self.district = self.keytuple[2]

        if self.first_time_change == False:
            self.Crimerecords[self.keytuple] = value

        self.oldkey = self.currentkey

if __name__ == '__main__':
    MRRatingCounter.run()
