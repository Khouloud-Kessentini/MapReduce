# Sorted Words Frequency Counter

from mrjob.job import MRJob
from mrstep.step import MRStep

class MRSortedWordsFrequencyCounter(MRJob):
  def steps(self):
    return[
      MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words),
      MRStep(mapper = self.mapper_make_counts, reducer = self.reducer_output_words)
    ]
  def mapper_get_words(self,_,line):
    words = line.split(" ")
    for word in words:
      yield word.lower()
  def reducer_count_words(self, key, value):
    yield key, sum(value)
  def mapper_make_counts(self, key, value):
    yield int(key), value
  def reducer_output_words(self, value, key):
    for word in value:
      yield key, word
if __name__ == '__main__':
  MRSortedWordsFrequencyCounter.run()
