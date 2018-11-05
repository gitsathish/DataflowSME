from __future__ import absolute_import

import argparse
import collections
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics

GameEvent = collections.namedtuple(
    'GameEvent', ['user', 'team', 'score', 'timestamp', 'event_id'])
PlayEvent = collections.namedtuple('PlayEvent',
                                   ['user', 'timestamp', 'event_id'])


class ParseEventFn(beam.DoFn):
    """Parses an event.
    [user,team,score,timestamp,readable_timestamp,event_id]
    """
    def __init__(self):
        super(ParseEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__,
                                                'num_event_parse_errors')

    def process(self, elem):
        try:
            parts = [x.strip() for x in elem.split(',')]
            user, team, score, timestamp = parts[:4]
            score = int(score)
            timestamp = long(timestamp)
            if len(parts) >= 6:
                event_id = parts[5]
            else:
                event_id = 'none'
            yield GameEvent(user, team, score, timestamp, event_id)
        except Exception as e:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s": %s', elem, str(e))


class ParsePlayEventFn(beam.DoFn):
    """Parses a play event: [user,timestamp,readable_timestamp,event_id]"""
    def __init__(self):
        super(ParsePlayEventFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__,
                                                'num_play_parse_errors')

    def process(self, elem):
        try:
            parts = [x.strip() for x in elem.split(',')]
            user, timestamp, _, event_id = parts[:5]
            yield PlayEvent(user, timestamp, event_id)
        except Exception as e:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s": %s', elem, str(e))


def ParseEvent(element):
    try:
        parts = [x.strip() for x in element.split(',')]
        user, team, score, timestamp = parts[:4]
        score = int(score)
        timestamp = long(timestamp)
        if len(parts) >= 6:
            event_id = parts[5]
        else:
            event_id = 'none'
        return [GameEvent(user, team, score, timestamp, event_id)]
    except:
        return []

def ParseArgs(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', help='Input file to process.')
    parser.add_argument(
        '--topic', dest='topic', help='Input topic to read from.')
    parser.add_argument(
        '--play_topic',
        dest='play_topic',
        help='Input topic to read for play events.')
    parser.add_argument(
        '--output_dataset',
        dest='output_dataset',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--output_tablename',
        dest='output_tablename',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--session_gap',
        dest='session_gap',
        help='Gap between user sessions, in seconds.')
    parser.add_argument(
        '--user_activity_window',
        dest='user_activity_window',
        help=
        'Value of fixed window for finding mean of session duration, in second.'
    )
    return parser.parse_known_args(argv)
