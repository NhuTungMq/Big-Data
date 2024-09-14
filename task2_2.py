from mrjob.job import MRJob
from mrjob.step import MRStep

class ThreeCountrieswiththeMostNumberofGoldMedals(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_split_words, reducer=self.reducer_count_sum_of_each_medals_types),
                MRStep(reducer=self.reducer_top_three_countries_most_gold)]
    
    def mapper_split_words(self, _, line):
        country = line.split(';')[1]
        medal = line.split(';')[-1]
        medal_type = {"Gold": (1, 0, 0), "Silver": (0, 1, 0), "Bronze": (0, 0, 1)}
        if medal in medal_type:
            yield country, medal_type[medal]
    
    def reducer_count_sum_of_each_medals_types(self, country, medals):
        total_gold = 0
        total_silver = 0
        total_bronze = 0 
        # Total medal counts for each country
        for medal in medals:
            total_gold += medal[0]
            total_silver += medal[1]
            total_bronze += medal[2]
        # Yield the country and its total medal counts
        yield None, (total_gold, total_silver, total_bronze, country)

    def reducer_top_three_countries_most_gold(self, _, country_medal_types):
        for gold, silver, bronze, country in sorted(country_medal_types, reverse=True, key=lambda x: x[0])[:3]:
            yield country, {"Gold": gold, "Silver": silver, "Bronze": bronze}

if __name__ == '__main__':
    ThreeCountrieswiththeMostNumberofGoldMedals.run()
