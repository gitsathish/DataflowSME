# This pipeline calculates the sum of scores per team per hour and writes the
# per-team sums to BigQuery. The pipeline can be run in either batch or
# streaming mode, reading from either a data file or Pub/Sub topic.
#
# You will need to create a Pub/Sub topic and run the Java Injector
# in order to get game events over Pub/Sub. Please refer to the instructions
# here: https://github.com/malo-denielou/DataflowSME
from __future__ import absolute_import

import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from util.util import GameEvent
from util.util import ParseEvent
from util.util import ParseEventFn
from util.util import ParseArgs
import apache_beam.transforms.window as window
from solutions.exercise1 import ExtractAndSumScore

# Defines the BigQuery schema.
SCHEMA = ('team:STRING,' 'total_score:INTEGER,' 'window_start:TIMESTAMP')


class ExtractAndSumScore(beam.PTransform):
    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, p):
        return (p
                | 'extract_field' >> beam.Map(
                    lambda x: (vars(x)[self.field], x.score))
                | beam.CombinePerKey(sum)
                )


class WindowedTeamScore(beam.PTransform):
    """A transform to compute a windowed team score."""
    def __init__(self, duration):
        super(WindowedTeamScore, self).__init__()
        self.duration = duration

    def expand(self, p):
        return (p
                | 'window' >> beam.WindowInto(
                    window.FixedWindows(self.duration))
                | 'extract_team_score' >> ExtractAndSumScore('team')
                )


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
    window_duration = 1 * 60  # 1 minute windows.
    if known_args.topic:
        pipeline_options.view_as(StandardOptions).streaming = True

    project = pipeline_options.view_as(GoogleCloudOptions).project
    timestamp_attribute = 'timestamp_ms'
    events = None
    if (not known_args.topic):
        events = (p
                | 'read' >> ReadFromText(known_args.input)
                | 'parse' >> beam.FlatMap(ParseEventFn())
                | 'add_event_timestamps' >> beam.Map(
                    lambda x: beam.window.TimestampedValue(x, x.timestamp))
                )
    else:
        events = (p
                | 'read' >> ReadFromPubSub(topic=known_args.topic,
                    timestamp_attribute='timestamp_ms')
                | 'decode' >> beam.ParDo(ParseEventFn())
                )

    _ = (events
            | 'windowed_team_score' >> WindowedTeamScore(window_duration)
            | 'format_team_score_sum' >> beam.ParDo(FormatTeamScoreSum())
            | beam.io.WriteToBigQuery(known_args.output_tablename,
                known_args.output_dataset, project, SCHEMA)
            )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
