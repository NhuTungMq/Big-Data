# from mrjob.job import MRJob
# from mrjob.step import MRStep

# class TopThreeCountries(MRJob):

#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_get_medals,
#                    reducer=self.reducer_count_medals),
#             MRStep(reducer=self.reducer_find_top_three)
#         ]

#     def mapper_get_medals(self, _, line):
#         # Split the line by commas
#         parts = line.split(',')
#         country = parts[1]  # Extract country code (e.g., "USA", "GBR")
#         medal_type = parts[-1]#.strip()  # Extract medal type (Gold, Silver, Bronze)
#         yield (country, medal_type), 1
    
#     def reducer_count_medals(self, country_medal, counts):
#         # Sum the count of each medal type for each country
#         yield country_medal[0], (country_medal[1], sum(counts))
    
#     def reducer_find_top_three(self, country, medal_counts):
#         # Initialize a dictionary to store the counts of each medal type
#         medal_totals = {"Gold": 0, "Silver": 0, "Bronze": 0}

#         # Accumulate the counts for each type of medal
#         for medal_type, count in medal_counts:
#             medal_totals[medal_type] += count

#         # Yield the country and its total medal counts as (gold_count, country, medal_totals)
#         yield None, (medal_totals["Gold"], country, medal_totals)
    
#     def reducer_output_top_three(self, _, country_medal_info):
#         # Sort the countries by gold medal count and take the top 3
#         top_three = sorted(country_medal_info, reverse=True)[:3]
        
#         # Directly print the results in the desired format
#         for gold_count, country, medal_totals in top_three:
#             # Output the format like in your image without 'null' and 'yield'
#             print(f'"{country}"\t{{"Gold":{medal_totals["Gold"]},"Silver":{medal_totals["Silver"]},"Bronze":{medal_totals["Bronze"]}}}')

# if __name__ == "__main__":
#     TopThreeCountries.run()

from mrjob.job import MRJob
from mrjob.step import MRStep

class MedalCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_medal_count,
                   reducer=self.reducer_country_medal_totals),
            MRStep(reducer=self.reducer_top_three_countries)
        ]
    
    def mapper_medal_count(self, _, line):
        # Split the input data by comma
        data = line.split(',')
        country = data[1]
        medal = data[-1]
        
        # Emit a key-value pair where the key is the country
        # and the value is a tuple representing the type of medal
        if medal == 'Gold':
            yield country, (1, 0, 0)  # 1 gold, 0 silver, 0 bronze
        elif medal == 'Silver':
            yield country, (0, 1, 0)  # 0 gold, 1 silver, 0 bronze
        elif medal == 'Bronze':
            yield country, (0, 0, 1)  # 0 gold, 0 silver, 1 bronze
    
    def reducer_country_medal_totals(self, country, medals):
        # Sum the gold, silver, and bronze medals for each country
        total_gold = 0
        total_silver = 0
        total_bronze = 0
        
        for gold, silver, bronze in medals:
            total_gold += gold
            total_silver += silver
            total_bronze += bronze
        
        # Emit the country and its total medal count as a tuple
        yield None, (total_gold, total_silver, total_bronze, country)
    
    def reducer_top_three_countries(self, _, country_medal_totals):
        # Sort countries by the number of gold medals in descending order
        sorted_countries = sorted(country_medal_totals, reverse=True, key=lambda x: x[0])
        
        # Output the top three countries in the desired format
        for i in range(3):
            if i < len(sorted_countries):
                country = sorted_countries[i][3]
                gold = sorted_countries[i][0]
                silver = sorted_countries[i][1]
                bronze = sorted_countries[i][2]
                # Yield as a dictionary, which will avoid the escaping issue
                yield country, {"Gold": gold, "Silver": silver, "Bronze": bronze}

if __name__ == '__main__':
    MedalCount.run()
