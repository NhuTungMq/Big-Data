from mrjob.job import MRJob
from mrjob.step import MRStep

class AthletesWhoWonTheMostNumberofMedals(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_split_words, reducer=self.reducer_sum_count_medals),
                MRStep(reducer=self.reducer_descending_order_medal_counts)]

    def mapper_split_words(self, _, line):
        # Split the line by semicolon
        words = line.split(';')
        athlete_id = words[0]  # Athlete ID
        medal_category = words[-1]
        yield (athlete_id, medal_category), 1

    def reducer_sum_count_medals(self, medal_category, counts):
        # Sum the counts for each athlete and medal type
        yield medal_category[1], (medal_category[0], sum(counts))
    
    def reducer_descending_order_medal_counts(self, athlete_medal_category, athlete_and_medal):
        # Sort athletes by total medals in descending order and yield the top three
        for athlete_id, medal_count in sorted(athlete_and_medal, key=lambda x: x[1], reverse=True)[:3]:
            yield athlete_medal_category, (athlete_id, medal_count)


if __name__ == "__main__":
    AthletesWhoWonTheMostNumberofMedals.run()
