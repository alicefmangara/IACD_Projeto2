# Data Processing with Hadoop MapReduce
# Task4: What is the sorted total amount spent by each customer?

from mrjob.job import MRJob
from mrjob.step import MRStep


class MR_customer(MRJob):
        
        def steps(self):
            return [
                MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.reducer_sort)
            ]
        
        def mapper(self, _, line):
            fields = line.split(',')
            customer = fields[0]
            spent = float(fields[2])
            yield customer, spent
        
        def reducer(self, customer, spent):
            yield customer, sum(spent)
        
        def reducer_sort(self, customer, spent):
            yield customer, sum(spent)

if __name__ == '__main__':
    MR_customer.run()

# Command to run the code:
# python MR_customer.py customers.csv

