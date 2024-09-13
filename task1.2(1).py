from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAthleteSort(MRJob):

    def steps(self):
        # Define the steps with two mapper and reducer functions
        return [
            MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
            MRStep(mapper=self.mapper_2, reducer=self.reducer_2)
        ]

    def mapper_1(self, _, line):
        # Split the line by commas
        parts = line.split(',')
        athlete_id = int(parts[0])  # Extract athlete ID
        yield athlete_id, parts[1:]  # Emit athlete ID as key and remaining data as value

    def reducer_1(self, athlete_id, data):
        # Pass the data to the next step, keeping the data grouped by athlete_id
        for item in data:
            yield athlete_id, item

    def mapper_2(self, athlete_id, data):
        # Prepare data for sorting, emit None as the key for sorting all entries together
        yield None, (athlete_id, data)

    def reducer_2(self, _, athlete_data):
        # Sort by athlete_id and output as a list
        for athlete_id, data in sorted(athlete_data):
            # Yield athlete_id as key and data as a list (not as a string)
            yield athlete_id, data  # Yield the list directly

if __name__ == "__main__":
    MRAthleteSort.run()
