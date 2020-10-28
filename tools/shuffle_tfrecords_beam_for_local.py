# Copyright 2018 Google LLC.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# pylint: disable=line-too-long
r"""Shuffle tf.Example files using beam.

To run locally:
1) Install beam on your machine following the instructions at
   https://beam.apache.org/get-started/quickstart-py/

2) Copy any inputs to be on local disk.

3) Run
  python path/to/shuffle_tfrecords_beam.py \
    --input_pattern_list="/tmp/some.examples-?????-of-00200.tfrecord.gz" \
    --output_pattern_prefix="/tmp/training.examples" \
    --output_dataset_name="HG001" \
    --runner=DirectRunner

To run on Google Cloud Dataflow Service:
1) Follow the Google Cloud Dataflow setup instructions at
https://beam.apache.org/documentation/runners/dataflow/

2) Upload this file to your GCE instance.

3) Run
  python shuffle_tfrecords_beam.py \
  --job_name=shuffle-tfrecords \
  --input_pattern_list="gs://YOUR_INPUT_BUCKET/A.tfrecord.gz" \
  --output_pattern_prefix="gs://YOUR_OUTPUT_BUCKET/training.examples" \
  --output_dataset_name="HG001" \
  --runner=DataflowRunner \
  --project=SET_YOUR_PROJECT_ID_HERE \
  --staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY \
  --temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY

4) (Optional) To monitor or cancel the job while it is running, you can
use either the Dataflow Monitoring Interface
https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf
or the Dataflow Command-line Interface
https://cloud.google.com/dataflow/pipelines/dataflow-command-line-intf
"""
# pylint: enable=line-too-long

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import glob

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
import os

COMMENT_HEADER = """# Generated by shuffle_tfrecords_beam.py
#
# --input_pattern_list={}
# --output_pattern_prefix={}
#
"""


def parse_cmdline(argv):
  """Parse the commandline into known and pipeline arguments.

  The known arguments are required for this specific program to function,
  and the other pipeline arguments can be used to configure beam and the
  specific beam backend being used.  See
  https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py
  for a list and description of the pipeline arguments accepted.

  Args:
    argv: List containing command-line arguments.

  Returns:
    A pair, the first of which are the known (non-pipeline) arguments
    and the second of which are the pipeline arguments.
  """
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--input_pattern_list',
      help='Comma-separated list of TFRecord filename patterns.')
  parser.add_argument(
      '--output_pattern_prefix',
      help='Filename pattern for the output TFRecords.')
  parser.add_argument(
      '--output_dataset_config_pbtxt',
      help='Optional.  If set, print out a human-readable version of '
      'DeepVariantDatasetConfig.')
  parser.add_argument(
      '--output_dataset_name',
      help='Optional unless --output_dataset_config_pbtxt is set.')
  parser.add_argument(
    '--num_buckets',
    help="Number for partitioning",
    default=1,
    type=int,
  )
  parser.add_argument(
    "--pre_partition",
    help="Pre-partition the input data into buckets",
    default=False,
    action="store_true",
  )
  parser.add_argument(
    "--pre_partition_workdir",
    help="Pre-partition work directory",
    required=False,
  )
  parser.add_argument(
    "--debug",
    help="Enable debug messages", action="store_true", default=False,
  )

  known_args, pipeline_args = parser.parse_known_args(argv)

  if known_args.pre_partition:
    assert(known_args.pre_partition_workdir is not None), "Provide working directory for pre-partition"
    assert(known_args.num_buckets > 0), "Minimum of one bucket should be provided for pre-partition"

  return known_args, pipeline_args


def partition(example, num_buckets):
  """ Partition examples into multiple buckets """
  def determine_hash(input_bytes):
    import hashlib
    m = hashlib.blake2b(input_bytes, digest_size=4)
    address = 0
    for i, b in enumerate(m.digest()):
      address += b * (2 ** (i * 8))
    return address

  address = determine_hash(example)
  bucket = address % num_buckets
  return bucket


def pre_partition(input_pattern, working_directory, pipeline_args, num_buckets):
  """
  Pre-partition data
  """
  if os.path.exists(working_directory):
    raise ValueError("Provide a clean working directory path directory")

  os.makedirs(working_directory)

  logging.info("Initiating pre-partitioning")

  with beam.Pipeline(options=PipelineOptions(pipeline_args)) as pipeline:
    input_data = read_from_tfrecords_files(pipeline, input_pattern)
    partitions = input_data | "PrePartition" >> beam.Partition(partition, num_buckets)
    for i, p in enumerate(partitions):
      write = p | "WritePartition%d" % i >> beam.io.WriteToTFRecord(
        file_path_prefix=os.path.join(
          working_directory, "partition%04d" % i), file_name_suffix=".tfrecord.gz", coder=coders.BytesCoder()
        )

  filenames = glob.glob(os.path.join(working_directory, "partition????-00000-of-00001.tfrecord.gz"))
  return filenames


def read_from_tfrecords_files(pipeline, input_filename_pattern_list):
  """Reads records from TFRecord files.

  Args:
    pipeline: Beam pipeline object.
    input_filename_pattern_list: List of filename patterns.

  Returns:
    A PCollection of read tf.Examples.
  """
  readers = []
  for i, filepattern in enumerate(input_filename_pattern_list):
    readers.append(pipeline
                   | 'ReadTFRecordFiles_{}[{}]'.format(i, filepattern) >> beam
                   .io.ReadFromTFRecord(filepattern, coder=coders.BytesCoder()))
  all_readers = readers | 'Flatten' >> beam.Flatten()
  return all_readers


def shuffle_records(input_examples):
  """Shuffles the input_examples in a effectively random order."""

  def sha1(input_bytes):
    """Returns the sha1 hash of input_bytes."""
    # SparkRunner requires this to be a lazy/local import
    import hashlib
    m = hashlib.sha1()
    m.update(input_bytes)
    return m.digest()

  return (input_examples
          | 'Randomize' >> beam.Map(lambda x: (sha1(x), x))
          | 'Groupby' >> beam.GroupByKey()
          | 'DropKey' >> beam.FlatMap(lambda x: x[1]))


# SparkRunner has some issue with using lambda functions in beam.Map
# Hence, this has been turned into a callable
class MakeConfigStringCaller:
  def __init__(self, dataset_name, output_pattern_prefix):
    self.name = dataset_name
    self.tfrecord_path = output_pattern_prefix

  def __call__(self, num_examples):
    # SparkRunner requires this to be a lazy/local import
    import textwrap
    return textwrap.dedent("""
    name: "{}"
    tfrecord_path: "{}-?????-of-?????.tfrecord.gz"
    num_examples: {}
    """.format(self.name, self.tfrecord_path, num_examples))


def write_summary_string_to_file(pipeline, output_examples, input_pattern_list,
                                 dataset_name, output_pattern_prefix,
                                 output_filename):
  """Writes a file summarizing the PCollection of Examples.

  Args:
    pipeline: Beam pipeline object.
    output_examples: PCollection of examples.
    input_pattern_list: str. A comma-separated string of input files.
    dataset_name: str. The name of the dataset to be written in the output.
    output_pattern_prefix: str. The prefix of the sharded output files.
    output_filename: the output text file that contains the summary that can be
      parsed into DeepVariantDatasetConfig.
  """

  # Beam currently has no way to materialize pipeline values, so we have
  # to construct the file entirely in Beam pipeline operations.
  comment_str = pipeline | 'CreateFileHeader' >> beam.Create(
      [COMMENT_HEADER.format(input_pattern_list, output_pattern_prefix)])
  num_examples = (
      output_examples
      | 'CountOutputExamples' >> beam.combiners.Count.Globally())
  config_str = num_examples | 'MakeConfigStr' >> beam.Map(
      MakeConfigStringCaller(dataset_name, output_pattern_prefix))

  merged_strings = (comment_str, config_str) | 'FlattenStrs' >> beam.Flatten()
  _ = (
      merged_strings
      | 'Concat2' >> beam.CombineGlobally(''.join)
      | 'WriteToFile' >> beam.io.WriteToText(
          output_filename, shard_name_template=''))


def main(argv=None):
  """Main entry point; defines and runs the pipeline."""
  known_args, pipeline_args = parse_cmdline(argv)
  if known_args.debug:
    logging.getLogger().setLevel(logging.DEBUG)
  else:
    logging.getLogger().setLevel(logging.INFO)

  # Prepartition files
  if known_args.pre_partition:
    input_files = pre_partition(
      known_args.input_pattern_list.split(","), known_args.pre_partition_workdir, pipeline_args, known_args.num_buckets
    )
  else:
    input_files = [known_args.input_pattern_list]

  # Shuffle each partition
  pipeline_options = PipelineOptions(pipeline_args)
  output_patterns = []
  
  for i, filename in enumerate(input_files):
    with beam.Pipeline(options=pipeline_options) as p:
      input_examples = read_from_tfrecords_files(
          p, filename.split(","))

      output_examples = shuffle_records(input_examples)
      output_pattern_prefix = known_args.output_pattern_prefix + "_partition%d" % i
      output_patterns.append(
        output_pattern_prefix + "-?????-of-?????.tfrecord.gz"
      )

      _ = output_examples | beam.io.WriteToTFRecord(
          file_path_prefix=output_pattern_prefix,
          file_name_suffix='.tfrecord.gz',
          coder=coders.BytesCoder())

  # Combine shuffled partitions into a single set of output files
  with beam.Pipeline(options=pipeline_options) as p:
    output_examples = read_from_tfrecords_files(p, output_patterns)
    write_outputs = output_examples | "WriteOutputs" >> beam.io.WriteToTFRecord(
      file_path_prefix=known_args.output_pattern_prefix,
      file_name_suffix=".tfrecord.gz",
      coder=coders.BytesCoder(),
    )

    if known_args.output_dataset_config_pbtxt:
      if not known_args.output_dataset_name:
        raise ValueError('Need to set output_dataset_name.')
      write_summary_string_to_file(p, output_examples,
                                   known_args.input_pattern_list,
                                   known_args.output_dataset_name,
                                   known_args.output_pattern_prefix,
                                   known_args.output_dataset_config_pbtxt)

  # Cleanup
  for pattern in output_patterns:
    for partial in glob.glob(pattern):
      os.remove(partial)


if __name__ == '__main__':
  main()
