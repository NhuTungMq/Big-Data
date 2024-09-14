from mrjob.job import MRJob
from mrjob.step import MRStep

class SortedAthlete(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_split_words, reducer=self.reducer_sorted_athlete_id)]

    def mapper_split_words(self, _, line):
        # Split the line by commas
        words = line.split(';')
        athlete_id = int(words[0])  # Extract athlete ID
        yield None, (athlete_id, words[1:])

    def reducer_sorted_athlete_id(self, _, athlete_sort):
        # Sort by athlete_id
        for athlete_id, data in sorted(athlete_sort):
            # Yield athlete_id as key and data as a list
            yield athlete_id, data

if __name__ == "__main__":
    SortedAthlete.run()
