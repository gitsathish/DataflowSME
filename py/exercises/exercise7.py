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
from util.util import ParsePlayEventFn
from util.util import ParseArgs

# Defines the BigQuery schemas.
SESSION_SCHEMA = ('window_start:TIMESTAMP,' 'mean_duration:FLOAT')


class ComputeLatency(beam.DoFn):
    def __init__(self):
        super(ComputeLatency, self).__init__()
        self.dropped_sessions_no_events = Metrics.counter(
            self.__class__, 'dropped_sessions_no_events')
        self.dropped_sessions_too_many_events = Metrics.counter(
            self.__class__, 'dropped_sessions_too_many_events')
        self.dropped_sessions_no_play_events = Metrics.counter(
            self.__class__, 'dropped_sessions_no_play_events')

    def process(self, elem):
        _, vals = elem
        plays = vals['plays']
        events = vals['events']

        play_count = 0
        max_play_ts = 0
        for play in plays:
            play_count += 1
            max_play_ts = max(max_play_ts, long(play.timestamp))

        event_count = 0
        an_event = None
        for event in events:
            an_event = event
            event_count += 1

        if event_count == 0:
            self.dropped_sessions_no_events.inc()
        elif event_count > 1:
            self.dropped_sessions_too_many_events.inc()
        elif play_count == 0:
            self.dropped_sessions_no_play_events.inc()
        else:
            min_latency = long(an_event.timestamp) - max_play_ts
            yield (an_event.user, min_latency)


class DetectBadUsers(beam.DoFn):
    def process(self, elem, mean_latency=beam.DoFn.SideInputParam):
        user, latency = elem
        # Naive: compute bad users are users 5 times less than
        # the mean.
        if latency < mean / 5:
            yield user


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
    if (not known_args.topic or not known_args.play_topic):
        logging.fatal('topic and play_topic are required.')

    # [START EXERCISE 7]:
    # 1. Read game events with message id and timestamp.
    # 2. Parse events.
    events = (p
            | 'read_events' >> ChangeMe()
            | 'parse_events' >> ChangeMe()
            )

    # 1. Read play events with message id and timestamp.
    # 2. Parse events.
    play_events = (p
            | 'read_play_events' >> ChangeMe()
            | 'parse_play_events' >> ChangeMe()
            )

    # 1. Key events by event id.
    # 2. Sessionize.
    sessionized_events = (events
            | 'key_events_by_id' >> ChangeMe()
            | 'sessionize_events' >> ChangeMe()

    # 1. Key play events by event id.
    # 2. Sessionize.
    sessionized_plays = (play_events
            | 'key_plays_by_id' >> ChangeMe()
            | 'sessionize_plays' >> ChangeMe()

    # 1. Join events using CoGroupByKey
    # 2. Compute latency using ComputeLatency
    per_user_latency = (
            {'change':me, 'me':change}
            | 'cbk' >> ChangeMe()
            | 'compute_latency' >> ChangeMe()

    # 1. Get values of per user latencies
    # 2. Re-window into GlobalWindows that triggers repeatedly after 1000 new elements.
    # 3. Compute the global mean to be used as a side input.
    mean_latency = (per_user_latency
            | 'extract_latencies' >> ChangeMe()
            | 'global_window' >> ChangeMe()
            | 'compute_mean' >> ChangeMe()
            )
    # [END EXERCISE 7]

    # Filter out bad users.
    _ = (per_user_latency
            | 'detect_bad_users' >> beam.ParDo(
                DetectBadUsers(), mean_latency=mean_latency)
            | 'filter_duplicates' >> beam.WindowInto(
                window.GlobalWindows(), trigger=trigger.AfterCount(1),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
            | 'to_bq_schema' >> beam.Map(lambda x: {'user': x})
            | 'write_bad_users' >> beam.io.WriteToBigQuery(
                known_args.output_tablename, known_args.output_dataset, project, ('user:string'))
            )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    Run()
