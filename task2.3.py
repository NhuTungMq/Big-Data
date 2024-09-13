from mrjob.job import MRJob
from mrjob.step import MRStep

class TopThreeEventsByDecade(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_count_medals),
            MRStep(reducer=self.reducer_top_three_events)
        ]

    def mapper(self, _, line):
        # Split the line by commas
        parts = line.split(',')
        country = parts[1]  # Extract country code (e.g., "USA", "GBR")
        event = ', '.join(parts[3:5])  # Extract event (e.g., "Basketball, Men")

        # Extract the year by finding the first integer in the event string
        try:
            year = int([s for s in parts[2].split() if s.isdigit()][0])  # Extract year properly
        except (IndexError, ValueError):
            year = None

        # If no valid year is found, skip this line
        if year is None:
            return

        # Calculate the decade
        decade = f"{year // 10 * 10}-{year // 10 * 10 + 9}"

        # We will sum the number of medals for each event, so yield the event, country, and decade
        yield (decade, country, event), 1

    def reducer_count_medals(self, event_key, counts):
        # Sum up the total number of medals for each event by country and decade
        total_medals = sum(counts)
        decade, country, event = event_key
        yield decade, (country, event, total_medals)

    def reducer_top_three_events(self, decade, country_event_counts):
        # Dictionary to hold total medal counts for each event
        event_dict = {}

        # Aggregate the total medals for each event in the decade by country
        for country, event, total_medals in country_event_counts:
            key = (country, event)
            if key in event_dict:
                event_dict[key] += total_medals
            else:
                event_dict[key] = total_medals

        # Sort the events by the total number of medals in descending order
        sorted_events = sorted(event_dict.items(), key=lambda x: x[1], reverse=True)

        # Output the top three events for each decade with the country
        top_three_events = sorted_events[:3]
        for (country, event), total_medals in top_three_events:
            yield decade, (country, event, total_medals)

if __name__ == "__main__":
    TopThreeEventsByDecade.run()
