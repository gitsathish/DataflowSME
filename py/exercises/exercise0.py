# This batch pipeline imports game events from CSV to BigQuery.
from __future__ import absolute_import

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from util.util import GameEvent
from util.util import ParseEvent
from util.util import ParseArgs

# Defines the BigQuery schema.
SCHEMA = ('user:STRING,' 'team:STRING,' 'score:INTEGER,' 'timestamp:TIMESTAMP')


def FormatEvent(element):
    """Format a GameEvent to a BigQuery TableRow."""
    return {
        'user': element.user,
        'team': element.team,
        'score': element.score,
        'timestamp': element.timestamp
    }


def Run(argv=None):
    """Run a batch pipeline."""
    known_args, pipeline_args = ParseArgs(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    project = pipeline_options.view_as(GoogleCloudOptions).project
    # Read events from a CSV file, parse them and write (import) them to BigQuery.
    _ = (p
        | 'read' >> ReadFromText(known_args.input)
        | 'parse' >> beam.FlatMap(ParseEvent)
        | 'format' >> beam.Map(FormatEvent)
        | beam.io.WriteToBigQuery(known_args.output_tablename,
            known_args.output_dataset, project, SCHEMA)
        )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
