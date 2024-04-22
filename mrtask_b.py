# Which pickup location generates the most revenue?

# Importing the necessary libraries
from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMapReduce(MRJob):

    #Creating the steps to be followed
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.output_reducer)
        ]        

    #This mapper code is for extracting the pickup location and all its revenues
    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            data = line.split(',')
            pu_location = data[7]
            revenue = float(data[16])
            yield pu_location, revenue        
    
    #This reducer code is for extracting the pickup location and sum of its respective revenues
    def reducer(self, pu_location, revenue):
        yield None, (sum(revenue), pu_location)
            
    #This reducer code is for finding the pickup location which has the maximum revenue
    def output_reducer(self, _, values):
         max_revenue, pu_location = max(values)
         yield pu_location, max_revenue


if __name__ == '__main__':
    MyMapReduce.run()