# This pipeline computes the average duration of user sessions. The
# averages are windowed, to reflect durations differing over time.
from __future__ import absolute_import

import logging
import re
import time

import apache_beam as beam
import apache_beam.transforms.window as window
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

# Defines the BigQuery schemas.
SESSION_SCHEMA = ('window_start:TIMESTAMP,' 'mean_duration:FLOAT')


class UserSessionActivity(beam.DoFn):
    """Compute the duration of a user's session."""
    def process(self,
                elem,
                timestamp=beam.DoFn.TimestampParam,
                window=beam.DoFn.WindowParam):
        duration = int(window.end) - int(window.start)
        yield duration


class FormatSessionMeans(beam.DoFn):
    """Format session means for output to BQ"""
    def process(self, elem, window=beam.DoFn.WindowParam):
        yield {'window_start': int(window.start), 'mean_duration': elem}


def Run(argv=None):
    known_args, pipeline_args = ParseArgs(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)
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
                | 'parse' >> beam.ParDo(ParseEventFn()))

    # [START EXERCISE 6]
    _ = (events
         | 'extract_user_score' >> beam.Map(lambda x: (x.user, x.score))
         # Extract sessions of user data, using known_args.session_gap as the
         # gap duration.
         # https://beam.apache.org/documentation/programming-guide/#provided-windowing-functions
         | 'sessionize' >> ChangeMe()
         | 'drop_scores' >> beam.CombinePerKey(lambda x: 0)
         | 'convert_to_activity' >> beam.ParDo(UserSessionActivity())
         # Re-window into fixed windows of size user_activity_window in order
         # to compute the mean session duration for that window of activity.
         | 'window_of_sessions' >> ChangeMe()
         | 'session_mean' >> ChangeMe()
         # [END EXERCISE 6]
         | 'format_sessions' >> beam.ParDo(FormatSessionMeans())
         | 'write_to_bigquery' >> beam.io.WriteToBigQuery(
             known_args.output_tablename, known_args.output_dataset, project,
             SESSION_SCHEMA)
         )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
