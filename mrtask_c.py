# What are the different payment types used by customers and their count? The final results should be in a sorted format.

# Importing the necessary libraries
from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMapReduce(MRJob):

    #Creating the steps to be followed
    def steps(self):
        return  [
            MRStep (mapper=self.mapper,
                    combiner=self.combiner,
                    reducer=self.reducer),
            MRStep (reducer=self.reducer_sort_results),
            MRStep (reducer=self.reducer_output_result)
            ]

    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            fields = line.split(',')
            payment_type = fields[9]
            yield payment_type, 1

    def combiner(self, payment_type, counts):
        yield payment_type, sum(counts)

    def reducer(self, payment_type, counts):
        yield payment_type, sum(counts)

    def reducer_sort_results(self, payment_type, counts):
        yield None, (sum(counts), payment_type)

    def reducer_output_result(self, _, sorted_results):
        for count, payment_type in sorted(sorted_results, reverse=True):
            yield payment_type, count


if __name__ == '__main__':
    MyMapReduce.run()
