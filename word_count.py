from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class word_count(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper1,
                       #combiner=self.combiner1,
                       reducer=self.reducer1),
                MRStep(mapper=self.mapper2,
                       #combiner=self.combiner2,
                       reducer=self.reducer2
                       )]
    
    def mapper1(self, key, value):
        value = re.sub("[^\w]", " ", value)
        words = value.split()
        for word in words:
            yield words, 1
        
    def reducer1(self, word, count):
        yield word, sum(count)
        
    def mapper2(self, word, word_count):
        yield "key", (word, word_count)
    
    def reducer2(self, _,word_count_list):
        for pair in sorted(word_count_list, key = lambda p: p[1]):
            key , value = pair
            yield key, value
            
if __name__ == '__main__':
    word_count.run()
