import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from store_app.fxns.parse_and_validate import ParseAndValidate,TupToDictStoreInfo 
from store_app.fxns.read_write_util import  write_to_sink, read_data_from_pubsub
from store_app.fxns.combine_product_by_store import CombineProductFn


def apply_transformations(lines):
    main, err = lines \
        | 'Parse and Validate' >> beam.ParDo(ParseAndValidate()).with_outputs(
            ParseAndValidate.ERR_REC,
            main='main')
    data_by_store = main \
        | 'Apply Fixed window' >> beam.WindowInto(beam.window.FixedWindows(10)) 
    data_by_store | beam.Map(print)
    d_k = data_by_store  | 'Group by store (Apply Transformation)' >>  beam.CombinePerKey(CombineProductFn())
    return d_k, err
    # | 'Apply Sliding window' >> WindowInto(window.SlidingWindows(120, 60),accumulation_mode=beam.trigger.AccumulationMode.DISCARDING) \


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_subscription',
      dest='input',
      required=True,
      help='Input Subscription.')
  parser.add_argument(
      '--output_table',
      dest='output_table',
      required=True,
      help='Output table')
  parser.add_argument(
      '--err_table',
      dest='err_table',
      required=True,
      help='Output error table')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=pipeline_options) as pipeline:
    lines = read_data_from_pubsub(pipeline=pipeline,input_subscription=known_args.input)
    data_by_store, err = apply_transformations(lines)
    data_table = data_by_store | 'Convert to Dict' >> beam.ParDo(TupToDictStoreInfo())
    write_to_sink(data_table,known_args.output_table, "Write output to BQ")
    write_to_sink(err,known_args.err_table, "Write error to BQ")

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()