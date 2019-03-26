
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRRatingCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper3)
        ]

    def mapper(self, key, value):
        line = value.replace(', ', '').strip()
        words = line.split(",")
        try:
            (NR, ID, CaseNumber, Date, Block, IUCR, PrimaryType, Description, LocationDescription, Arrest, Domestic, Beat,District, Ward, CommunityArea, FBICode, XCoordinate, YCoordinate, YEAR, UpdatedOn, Latitude, Longitude,Location) = words
            yield (YEAR, PrimaryType), 1
        except ValueError:
            pass

    def reducer(self, YearType, occurrences):
        yield YearType, sum(occurrences)

        # ["2001", "ARSON"] 3
        # ["2001", "ASSAULT"] 72
        # ["2001", "BATTERY"] 207

    def mapper_init(self):
        self.oldkey = None
        self.currentkey = None
        self.maxdif = 0
        self.Crimerecords = {}
        self.keytuple = ""
        self.first_time_change = False
        self.most_reduced_crime = None

    def mapper3(self, key, value):

        self.keytuple = int(key[0]), key[1]   # convert the key to string format for saving it as a dict key
        self.currentkey = int(key[0])         # Extract the year from the key

        if self.oldkey and self.oldkey != self.currentkey and self.first_time_change:
            yield (self.oldkey - 1, self.oldkey), (self.most_reduced_crime, self.maxdif)
            self.maxdif = 0
            self.most_reduced_crime = None

        if self.oldkey and self.oldkey != self.currentkey and self.first_time_change == False:
            self.first_time_change = True

        if self.first_time_change:
            self.Crimerecords[self.keytuple] = value

            str1 = self.keytuple[0] - 1   #save (year - 1) of the tuple
            str2 = self.keytuple[1]       #save Typeofcrime of the tuple

            self.keytuple = str1, str2

            if self.keytuple in self.Crimerecords:
                if self.Crimerecords[self.keytuple] - value > self.maxdif:
                    self.maxdif = self.Crimerecords[self.keytuple] - value
                    self.most_reduced_crime = self.keytuple[1]

        if self.first_time_change == False:
            self.Crimerecords[self.keytuple] = value

        self.oldkey = self.currentkey

if __name__ == '__main__':
    MRRatingCounter.run()
