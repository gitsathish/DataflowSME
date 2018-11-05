# This pipeline calculates the sum of scores per team per hour and writes the
# per-team sums to BigQuery. Additionally computes running user scores (e.g.,
# as a leaderboard) and updates them regularly.

# The pipeline can be run in either batch or streaming mode, reading from
# either a data file or Pub/Sub topic.
from __future__ import absolute_import

import logging
import re
import time

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
from apache_beam.transforms import trigger
from util.util import GameEvent
from util.util import ParseEvent
from util.util import ParseEventFn
from util.util import ParseArgs
import apache_beam.transforms.window as window
from solutions.exercise1 import ExtractAndSumScore

# Defines the BigQuery schemas.
USER_SCHEMA = ('user:STRING,'
               'total_score:INTEGER,'
               'processing_time:TIMESTAMP')
TEAM_SCHEMA = ('team:STRING,' 'total_score:INTEGER,' 'window_start:TIMESTAMP')


class ExtractAndSumScore(beam.PTransform):
    def __init__(self, field):
        super(ExtractAndSumScore, self).__init__()
        self.field = field

    def expand(self, p):
        return (p | 'extract_field' >>
                beam.Map(lambda x: (vars(x)[self.field], x.score)) |
                beam.CombinePerKey(sum))


class RunningUserScores(beam.PTransform):
    """Extract user/score pairs via global windowing and emit perioidic updates
     on all users' running scores.
    """
    def __init__(self, allowed_lateness=0):
        super(RunningUserScores, self).__init__()

    def expand(self, p):
        # NOTE: allowed_lateness is not yet available in Python FixedWindows.
        # NOTE: AfterProcessingTime not yet available in Python.
        # [START EXERCISE 4.1]:
        # Compute a leaderboard by windowing user scores into the global window.
        # Since we will want to see running results, trigger the window early,
        # after every 100 elements. Make sure to accumulate fired panes.
        # https://beam.apache.org/documentation/programming-guide/#triggers
        return (p
                | 'window' >> ChangeMe()
                | 'extract_user_score' >> ExtractAndSumScore('user')
                )
        # [END EXERCISE 4.1]


class WindowedTeamScore(beam.PTransform):
    """Calculates scores for each team within the configured window duration"""

    def __init__(self, duration):
        super(WindowedTeamScore, self).__init__()
        self.duration = duration

    def expand(self, p):
        # [START EXERCISE 4.2]:
        # Window team scores into windows of fixed duration. Trigger these windows
        # on-time with the watermark, but also speculatively every 100 elements.
        # Ensure correct totals for the watermark-triggered pane by accumulating
        # over all data.
        return (p
                | 'window' >> ChangeMe()
                | 'extract_team_score' >> ExtractAndSumScore('team')
                )
        # [END EXERCISE 4.2]


class FormatTeamScoreSum(beam.DoFn):
    """Format a KV of team and its score to a BigQuery TableRow."""
    def process(self, team_score, window=beam.DoFn.WindowParam):
        team, score = team_score
        start = int(window.start)
        yield {
            'team': team,
            'total_score': score,
            'window_start': start,
        }


class FormatUserScoreSum(beam.DoFn):
    """Format a KV of user and their score to a BigQuery TableRow."""
    def process(self, user_score, window=beam.DoFn.WindowParam):
        user, score = user_score
        yield {
            'user': user,
            'total_score': score,
            'processing_time': time.time(),
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

    # Window team scores and write them BigQuery.
    _ = (events
         | 'windowed_team_score' >> WindowedTeamScore(window_duration)
         | 'format_team_score_sum' >> beam.ParDo(FormatTeamScoreSum())
         | 'write_teams_to_bigquery' >> beam.io.WriteToBigQuery(
             known_args.output_tablename + '_team', known_args.output_dataset,
             project, TEAM_SCHEMA)
         )

    # Write leaderboards to BigQuery.
    _ = (events
         | 'running_user_score' >> RunningUserScores()
         | 'format_user_scores' >> beam.ParDo(FormatUserScoreSum())
         | 'write_users_to_bigquery' >> beam.io.WriteToBigQuery(
             known_args.output_tablename + '_user', known_args.output_dataset,
             project, USER_SCHEMA)
         )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
