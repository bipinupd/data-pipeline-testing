import apache_beam as beam
from apache_beam import DoFn
from apache_beam import pvalue

class DivisibleByDoFn(DoFn):
  def process(self, element):
    if element % 2 == 0:
      yield pvalue.TaggedOutput("divisible_by", element)
    else:
      yield pvalue.TaggedOutput("not_divisible_by", element)

