import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from store_app.fxns.parse_and_validate import TupToDictStoreInfo, ParseAndValidate
import store_app.fxns.read_write_util as util
from store_app.fxns.combine_product_by_store import CombineProductFn

def apply_transformations(lines):
    main, err = lines \
        | 'Parse and Validate' >> beam.ParDo(ParseAndValidate()).with_outputs(
            ParseAndValidate.ERR_REC,
            main='main')
    data_by_store = main \
        | 'Group by store (Apply Transformation)' >>  beam.CombinePerKey(CombineProductFn())
    return data_by_store, err

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')
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

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as pipeline:
    lines = util.read_data_from_gcs(pipeline=pipeline,gcs_bucket=known_args.input)
    data, err = apply_transformations(lines)
    data_table = data | 'Convert to Dict' >> beam.ParDo(TupToDictStoreInfo())
    util.write_to_sink(data_table,known_args.output_table, "Write output to BQ")
    util.write_to_sink(err,known_args.err_table, "Write error to BQ")
     
if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  run()