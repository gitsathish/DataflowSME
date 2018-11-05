# Filter 'cheating' or 'spammy' users from the game results.
# Computes the global mean score and filters users that are 
# some threshold above that score.
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
        return (p
                | 'extract_field' >> beam.Map(
                    lambda x: (vars(x)[self.field], x.score))
                | beam.CombinePerKey(sum)
                )


class WindowedUserScores(beam.PTransform):
    """Extract user/score pairs via in fixed windows."""
    def __init__(self, duration):
        super(WindowedUserScores, self).__init__()
        self.duration = duration

    def expand(self, p):
        return (p
                | 'window' >> beam.WindowInto(
                    window.FixedWindows(self.duration))
                | 'extract_user_score' >> ExtractAndSumScore('user')
                )


class FilterUser(beam.DoFn):
    """Filter a user if their score * score_weight > avg_score."""
    def __init__(self, score_weight):
        super(FilterUser, self).__init__()
        self.score_weight = score_weight
        self.num_spammy_users = Metrics.counter(self.__class__,
                                                'num_spammy_users')

    def process(self, user_score, avg_score=beam.DoFn.SideInputParam):
        user, score = user_score
        if score * self.score_weight > avg_score:
            logging.error('User %s filtered as spammy', user)
            self.num_spammy_users.inc()
            yield user


class ComputeSpammyUsers(beam.PTransform):
    """Compute users with a high clickrate, which we will consider spammy.
     We do this by finding the mean total score per user and filter out
     those with scores that are greater than the mean * score_weight
    """
    def __init__(self, score_weight):
        super(ComputeSpammyUsers, self).__init__()
        self.score_weight = score_weight

    def expand(self, p):
        avg_score = (p
                | beam.Values()
                | beam.CombineGlobally(
                    beam.combiners.MeanCombineFn()).as_singleton_view()
                )
        return (p
                | 'compute_spammers' >> beam.ParDo(
                    FilterUser(self.score_weight), avg_score=avg_score)
                )


class FilterSpammers(beam.DoFn):
    """Remove users found in the spam list."""
    def __init__(self):
        super(FilterSpammers, self).__init__()
        self.filtered_scores = Metrics.counter(self.__class__,
                                               'filtered_scores')

    def process(self, elem, spammers=beam.DoFn.SideInputParam):
        user = elem.user
        if user not in spammers:
            yield elem
        else:
            self.filtered_scores.inc()


class WindowedTeamScore(beam.PTransform):
    """Calculates scores for each team within the configured window duration"""
    def __init__(self, duration, spammers):
        super(WindowedTeamScore, self).__init__()
        self.duration = duration
        self.spammers = spammers

    def expand(self, p):
        return (p
                | 'window' >> beam.WindowInto(
                    window.FixedWindows(self.duration))
                | 'filter_spammers' >> beam.ParDo(
                    FilterSpammers(), spammers=self.spammers)
                | 'extract_team_score' >> ExtractAndSumScore('team')
                )


class FormatTeamScoreSum(beam.DoFn):
    def process(self, team_score, window=beam.DoFn.WindowParam):
        team, score = team_score
        start = int(window.start)
        yield {
            'team': team,
            'total_score': score,
            'window_start': start,
        }


class FormatUserScoreSum(beam.DoFn):
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
                    lambda x: beam.window.TimestampedValue(x, x.timestamp)))
    else:
        events = (p
                | 'read' >> ReadFromPubSub(
                    topic=known_args.topic,
                    timestamp_attribute='timestamp_ms')
                | 'decode' >> beam.ParDo(ParseEventFn()))

    user_scores = (events
            | 'window_user_scores' >> WindowedUserScores(window_duration))
    spammers = beam.pvalue.AsList(user_scores
            | 'compute_spammers' >> ComputeSpammyUsers(2.5))

    _ = (events
         | 'windowed_team_score' >> WindowedTeamScore(window_duration, spammers)
         | 'format_team_score_sum' >> beam.ParDo(FormatTeamScoreSum())
         | 'write_teams_to_bigquery' >> beam.io.WriteToBigQuery(
             known_args.output_tablename, known_args.output_dataset, project,
             TEAM_SCHEMA)
         )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
