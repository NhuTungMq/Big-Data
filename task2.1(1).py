from mrjob.job import MRJob
from mrjob.step import MRStep

class TopThreeMedals(MRJob):

    def steps(self):
        # Breaking down into more steps
        return [
            MRStep(mapper=self.mapper_extract_data, reducer=self.reducer_validate_years),
            MRStep(reducer=self.reducer_count_medals),
            MRStep(reducer=self.reducer_top_three)
        ]

    def mapper_extract_data(self, _, line):
        # Step 1: Extract athlete_id, event, and medal_type from the line
        parts = line.split(',')
        athlete_id = parts[0]  # Athlete ID
        event = parts[2]  # Event information
        medal_type = parts[-1].strip()  # Medal type
        
        # Yield athlete_id and event, medal_type for further processing
        yield athlete_id, (event, medal_type)

    def reducer_validate_years(self, athlete_id, events_medals):
        # Step 2: Validate years and filter out those outside 1980-2020
        for event, medal_type in events_medals:
            year = self.extract_year(event)

            if year and 1980 <= year <= 2020:
                # Only yield valid years
                yield (athlete_id, medal_type), 1

    def extract_year(self, event):
        # Step 3: Manually extract year from event string without using isdigit()
        words = event.split()
        for word in words:
            # Check if the word can be converted to a valid integer year (e.g., "1990")
            if self.is_year(word):
                return int(word)  # Return the year
        return None  # Return None if no valid year is found

    def is_year(self, word):
        # Helper function: Check if the word can represent a year manually
        if len(word) == 4:  # Consider valid years to be 4 digits long
            for char in word:
                if char not in "0123456789":  # Check if each character is a digit
                    return False
            return True
        return False

    def reducer_count_medals(self, athlete_medal_type, counts):
        # Step 4: Count medals for each athlete by medal type
        total_medals = sum(counts)
        athlete_id, medal_type = athlete_medal_type
        yield medal_type, (athlete_id, total_medals)

    def reducer_top_three(self, medal_type, athlete_medal_pairs):
        # Step 5: Sort athletes by total medals and yield top three
        sorted_athletes = sorted(athlete_medal_pairs, key=lambda x: x[1], reverse=True)
        top_three = sorted_athletes[:3]
        for athlete in top_three:
            yield medal_type, athlete

if __name__ == '__main__':
    TopThreeMedals.run()
