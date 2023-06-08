import json
import argparse
import apache_beam as beam
from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import WindowInto
from apache_beam import window
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ExtractAndSummarize(beam.DoFn):

    def process(self, element):
        min_timestamp = None
        max_timestamp = None
        count = 0
        min_timestamp = ""
        max_timestamp = ""
        for ele in element[1]:
            if (count == 0):
                min_timestamp = ele['timestamp']
                max_timestamp = ele['timestamp']
            else:
                if ele['timestamp'] < min_timestamp:
                    min_timestamp = ele['timestamp']
                if ele['timestamp'] > max_timestamp:
                    max_timestamp = ele['timestamp']
            count = count + ele["passenger_count"]
        retVal = {
            "ride_status": element[0],
            "passenger_count": count,
            "min_timestamp": min_timestamp,
            "max_timestamp": max_timestamp
        }
        yield retVal


class ParseMessages(DoFn):
    ERR_REC = "ERROR_IN_STEP_ParseMessages"

    def process(self, element):
        try:
            parsed = json.loads(element.decode("utf-8"))
            retVal = {
                'timestamp': parsed['timestamp'],
                'passenger_count': parsed['passenger_count']
            }
            yield (parsed['ride_status'], retVal)
        except BaseException as error:
            json_err = {
                "payload": element,
                "error": str(error),
                "error_step_id": self.ERR_REC
            }
            yield beam.pvalue.TaggedOutput(self.ERR_REC, json_err)


def read_data_from_pubsub(pipeline, input_subscription):
    return (pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=input_subscription, timestamp_attribute='ts'))


def apply_transformations(lines):
    main, error = lines | "ParseMessages" >> ParDo(
        ParseMessages()).with_outputs(ParseMessages.ERR_REC, main='main')
    data_with_store = main | "ApplySlidingWindow" >> WindowInto(window.SlidingWindows(30, 20)) \
                          | "Group by key" >> beam.GroupByKey() \
                          | "Summarize" >> ParDo(ExtractAndSummarize())
    return data_with_store, error


def write_to_sink(pcoll, output_topic):
    return  pcoll \
          | 'encode' >> beam.Map(lambda x: json.dumps(x).encode('utf-8')).with_output_types(bytes) \
          | "Write to Sink" >> beam.io.WriteToPubSub(output_topic)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription',
                        dest='input_subscription',
                        required=True,
                        help='Input subscription')
    parser.add_argument('--output_topic',
                        dest='output_topic',
                        required=True,
                        help='Output topic')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Defining our pipeline and its steps
    with Pipeline(options=pipeline_options) as p:
        lines = read_data_from_pubsub(p, known_args.input_subscription)
        data_with_store, error = apply_transformations(lines)
        write_to_sink(data_with_store, known_args.output_topic)


if __name__ == "__main__":
    run()
