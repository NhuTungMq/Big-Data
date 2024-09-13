from mrjob.job import MRJob
from mrjob.step import MRStep

class TopThreeMedals(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_1, reducer=self.reducer_count_medals),
            MRStep(reducer=self.reducer_top_three)
        ]

    def mapper_1(self, _, line):
        # Split the line by commas
        types = line.split(',')
        athlete_id = types[0]  # Athlete ID
        medal_type = types[-1].strip()  # Last element is the medal type (Gold, Silver, Bronze)
        # Emit athlete_id and medal_type as key-value pair
        yield (athlete_id, medal_type), 1

    def reducer_count_medals(self, athlete_medal_type, counts):
        # Aggregate medal counts for each athlete and medal type
        athlete_id, medal_type = athlete_medal_type
        # Sum the counts for each athlete and medal type
        yield medal_type, (athlete_id, sum(counts))

    def reducer_top_three(self, medal_type, athlete_medal_pairs):
        # Sort athletes by total medals in descending order and get the top three
        sorted_athletes = sorted(athlete_medal_pairs, key=lambda x: x[1], reverse=True)
        top_three = sorted_athletes[:3]
        
        # Yield the top three athletes for each medal type
        for athlete_id, medal_count in top_three:
            yield medal_type, (athlete_id, medal_count)

if __name__ == "__main__":
    TopThreeMedals.run()
