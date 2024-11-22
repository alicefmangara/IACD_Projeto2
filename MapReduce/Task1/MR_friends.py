# Data Processing with Hadoop MapReduce
# Task1: What is the average number of friends by age?

from mrjob.job import MRJob
from mrjob.step import MRStep


class MR_friends(MRJob):
        
        def steps(self):
            return [
                MRStep(mapper=self.mapper, reducer=self.reducer)
            ]
        
        def mapper(self, _, line):
            fields = line.split(',')
            age = int(fields[2])
            num_friends = int(fields[3])
            yield age, num_friends
            
        def reducer(self, age, num_friends):
            friends = list(num_friends)
            yield age, sum(friends)/len(friends)

if __name__ == '__main__':
    MR_friends.run()

# Command to run the code:
# !python MR_friends.py fakefriends.csv

# libraries needed to run the code
# !pip install mrjob


