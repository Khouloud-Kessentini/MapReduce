from mrjob.job import MRJob

class MRWordFrequencyCounter(MRJob):
    def mapper(self,_,line):
        words = line.split(' ')
        for word in words:
            yield word.lower(), 1
    def reducer(self, key, value):
        yield key, sum(value)
if __name__ == "__main__":
    MRWordFrequencyCounter.run()
