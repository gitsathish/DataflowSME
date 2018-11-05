# This batch pipeline calculates the sum of scores per team per hour, over an
# entire batch of gaming data and writes the per-team sums to BigQuery.
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
import apache_beam.transforms.window as window

# Defines the BigQuery schema.
SCHEMA = ('team:STRING,' 'total_score:INTEGER,' 'window_start:TIMESTAMP')


class ExtractAndSumScore(beam.PTransform):
    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, p):
        return (p
                |'extract_field' >> beam.Map(lambda x: (vars(x)[self.field], x.score))
                | beam.CombinePerKey(sum)
                )


class WindowedTeamScore(beam.PTransform):
    """A transform to compute the WindowedTeamScore."""
    def __init__(self, duration):
        super(WindowedTeamScore, self).__init__()
        self.duration = duration

    def expand(self, p):
        # [START EXERCISE 2]:
        # Developer Docs: https://beam.apache.org/documentation/programming-guide/#windowing
        # Also: https://cloud.google.com/dataflow/model/windowing
        return (p
                # beam.WindowInto takes a WindowFn and returns a PTransform that applies windowing.
                # window.FixedWindows returns a WindowFn that assigns elements into fixed-size
                # windows. Use these methods to apply windows of size self.duration.
                | 'window' >> ChangeMeTransform()
                # Use the ExtractAndSumScore to compute the 'team' sum.
                | 'extract_team_score' >> ChangeMeTransform()
                )
        # [END EXERCISE 2]


class FormatTeamScoreSum(beam.DoFn):
    """Format a KV of user and their score to a BigQuery TableRow."""
    def process(self, team_score, window=beam.DoFn.WindowParam):
        team, score = team_score
        start = int(window.start)
        yield {
            'team': team,
            'total_score': score,
            'window_start': start,
        }


def Run(argv=None):
    known_args, pipeline_args = ParseArgs(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    project = pipeline_options.view_as(GoogleCloudOptions).project
    _ = (p
         | 'read' >> ReadFromText(known_args.input)
         | 'parse' >> beam.FlatMap(ParseEvent)
         | 'add_event_timestamps' >> beam.Map(
             lambda x: beam.window.TimestampedValue(x, x.timestamp))
         | 'windowed_team_score' >> WindowedTeamScore(60 * 60)
         | 'format_team_score_sum' >> beam.ParDo(FormatTeamScoreSum())
         | beam.io.WriteToBigQuery(known_args.output_tablename,
             known_args.output_dataset, project, SCHEMA)
         )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
