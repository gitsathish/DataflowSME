# This batch pipeline calculates the sum of scores per user, over an entire batch of gaming data and writes the sums to BigQuery.
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
SCHEMA = ('user:STRING,' 'total_score:INTEGER')


class ExtractAndSumScore(beam.PTransform):
    """A transform to extract key/score information from GameEvent, and sum
     the scores. The constructor arg determines whether 'team' or 'user' info is
     extracted."""
    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, p):
        return (p
                | 'extract_field' >> beam.Map(lambda x: (vars(x)[self.field], x.score))
                | beam.CombinePerKey(sum)
                )


def FormatUserScoreSum(element):
    """Format a KV of user and their score to a BigQuery TableRow."""
    user, total_score = element
    return {'user': user, 'total_score': total_score}


def Run(argv=None):
    known_args, pipeline_args = ParseArgs(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    project = pipeline_options.view_as(GoogleCloudOptions).project
    # Read events from a CSV file and parse them.
    _ = (p
         | 'read' >> ReadFromText(known_args.input)
         | 'parse' >> beam.FlatMap(ParseEvent)
         | 'extract_user_score' >> ExtractAndSumScore('user')
         | 'format_user_score_sum' >> beam.Map(FormatUserScoreSum)
         | beam.io.WriteToBigQuery(known_args.output_tablename,
             known_args.output_dataset, project, SCHEMA)
         )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
