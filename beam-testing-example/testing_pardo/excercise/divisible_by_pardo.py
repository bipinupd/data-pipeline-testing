import apache_beam as beam
from apache_beam import DoFn
from apache_beam import pvalue


class DivisibleByDoFn(DoFn):

    def __init__(self, number):
        self.number = number

    def process(self, element):
        try:
            if element % self.number == 0:
                yield pvalue.TaggedOutput("divisible_by", element)
            else:
                yield pvalue.TaggedOutput("not_divisible_by", element)
        except Exception as exception:
            json_err = {
                "payload": element,
                "error": str(exception),
                "error_step_id": "DivisibleByDoFn"
            }
            yield beam.pvalue.TaggedOutput("error", json_err)
