# Words Frequency Counter (REGEX)

from mrjob.job import MRJob
import re

WORD = re.compile(r"[\w']+")

class MRWordFrequencyCounter(MRJob):
    def mapper(self,_,line):
        words = WORD.findall(line)
        for word in words:
            yield word.lower(), 1
    def reducer(self, key, value):
        yield key, sum(value)
if __name__ == "__main__":
    MRWordFrequencyCounter.run()
