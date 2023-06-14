import apache_beam as beam
import argparse
from apache_beam import Pipeline
from apache_beam.io import ReadFromText
import re
from apache_beam.io import WriteToText

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)


def get_input_from_gcs(pipeline, gcs_location):
    return (pipeline | 'Read' >> ReadFromText(gcs_location))


def write_to_gcs(pcoll, gcs_location, label):
    return pcoll | 'Wrtie to GCS ' + label >> WriteToText(gcs_location)


def filter_using_length(word, lower_bound, upper_bound=float('inf')):
    if lower_bound <= len(word) <= upper_bound:
        yield word


def apply_transformations(lines, small_word_min, small_word_max):
    words = lines | 'Split' >> (beam.ParDo(
        WordExtractingDoFn()).with_output_types(str))
    # Construct a deferred side input.
    avg_word_len = (words | beam.Map(len) |
                    beam.CombineGlobally(beam.combiners.MeanCombineFn()))
    # Call with explicit side inputs.
    small_words = words | 'small' >> beam.FlatMap(filter_using_length, small_word_min, small_word_max) \
                        | 'PairWithOne For Small Words' >> beam.Map(lambda x: (x, 1)) \
                        | 'GroupAndSum for small words' >> beam.CombinePerKey(sum)

    # A single deferred side input.
    larger_than_average = words | 'large' >> beam.FlatMap(filter_using_length, lower_bound=beam.pvalue.AsSingleton(avg_word_len)) \
            | 'PairWithOne For Larger than avg Words' >> beam.Map(lambda x: (x, 1)) \
            | 'GroupAndSum for Larger than avg words' >> beam.CombinePerKey(sum)

    return (small_words, larger_than_average)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input bucket')
    parser.add_argument('--output', dest='output', help='Output bucket')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with Pipeline(options=pipeline_options) as p:
        lines = get_input_from_gcs(p, known_args.input)
        small_words, larger_than_average = apply_transformations(lines, 1, 3)
        write_to_gcs(small_words,
                     f"{known_args.output}/small_words",
                     label="small word")
        write_to_gcs(larger_than_average,
                     f"{known_args.output}/larger_than_average_words",
                     label="large words")


if __name__ == "__main__":
    run()
