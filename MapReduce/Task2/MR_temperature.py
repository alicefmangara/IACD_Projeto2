# Data Processing with Hadoop MapReduce
# Task2: What is the minimum temperature for each capital?

from mrjob.job import MRJob
from mrjob.step import MRStep


class MR_temperature(MRJob):
            
            def steps(self):
                return [
                    MRStep(mapper=self.mapper, reducer=self.reducer)
                ]
            
            def mapper(self, _, line):
                fields = line.split(',')
                capital = fields[0]
                temperature = int(fields[1])
                yield capital, temperature
                
            def reducer(self, capital, temperature):
                temp = list(temperature)
                yield capital, min(temp)

if __name__ == '__main__':
    MR_temperature.run()

# Command to run the code:
# python MR_temperature.py capitals.csv
