import apache_beam as beam
from apache_beam import DoFn
from apache_beam import pvalue

# Elements in CSV format containing
# transcation_date, transcation_id, store_id, prodcut_name, qty, amount


class ParseAndValidate(beam.DoFn):
    ERR_REC = "error_at_stage_ParseAndValidate"

    def process(self, element):
        try:
            data = element.split(",")
            if (len(data)) == 6:
                yield beam.pvalue.TaggedOutput("main",\
                    (data[2], (data[0],data[3], int(data[4]),float(data[5]))))
            else:
                raise Exception(
                    "Invalid data. Does not match the number of elements")
        except Exception as exception:
            json_err = {
                "payload": element,
                "error": str(exception),
                "error_step_id": self.ERR_REC
            }
            yield beam.pvalue.TaggedOutput("error", json_err)
