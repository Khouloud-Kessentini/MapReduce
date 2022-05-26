# Max Temperature

from mrjob.job import MRJob
class MRMaxTemperature(MRJob):
  def mapper(self,_,line):
    localization, x, temperature_type, temperature, y, a, b, c = line.split(",")
    if (temperature_type == "TMAX"):
      yield localization, temperature
  def reducer(self,key,value):
    yield key, max(value)
if __name__ == "__main__":
  MRMaxTemperature.run()
