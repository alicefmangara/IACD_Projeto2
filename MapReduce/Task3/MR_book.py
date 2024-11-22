# Data Processing with Hadoop MapReduce
# Task3: What is the sorted word frequency in the book?

import re

from mrjob.job import MRJob
from mrjob.step import MRStep

WORD_RE = re.compile(r"[\w']+")

class MR_book(MRJob):
        
        def steps(self):
            return [
                MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.reducer_sort)
            ]
        
        def mapper(self, _, line):
            words = WORD_RE.findall(line)
            for word in words:
                yield word.lower(), 1
        
        def reducer(self, word, counts):
            yield word, sum(counts)
        
        def reducer_sort(self, word, counts):
            yield word, sum(counts)

if __name__ == '__main__':
    MR_book.run()

# Command to run the code:
# python MR_book.py book.txt
