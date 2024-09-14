from mrjob.job import MRJob
from mrjob.step import MRStep

class EventsWithHighestMedalCountsforEachDecade(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_split_words,reducer=self.reducer_count),
                MRStep(reducer=self.reducer_top_three_events_each_decade),
                MRStep(reducer=self.reducer_list_events),
                MRStep(reducer=self.reducer_sorted_decade)]

    def mapper_split_words(self, _, line):
        words = line.split(';')
        country = words[1]
        year = int(words[2])
        event = words[3]
        decade = f"{year // 10 * 10}-{year // 10 * 10 + 9}"
        medal_value = 1
        yield (decade, country, event), medal_value


    def reducer_count(self, word, counts):
        # Aggregate medal counts for each decade, country and event
        yield word[0], (word[1], word[2], sum(counts))

    def reducer_top_three_events_each_decade(self, decade, events):
        # Sort events within each decade by medal count and yield the top three
        for event in sorted(events, key=lambda x: x[2], reverse=True)[:3]:
            yield decade, event

    def reducer_list_events(self, decade, events):
        # Gather all events in each the decade to sort them later
        yield None, (decade, list(events))

    def reducer_sorted_decade(self, _, decade_events):
        # Sort decades in descending order and output the events
        for decade, events in sorted(decade_events, key=lambda x: x[0], reverse=True):
            for event in events:
                yield decade, event

if __name__ == '__main__':
    EventsWithHighestMedalCountsforEachDecade.run()
