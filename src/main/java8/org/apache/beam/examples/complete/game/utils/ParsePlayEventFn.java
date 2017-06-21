/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.examples.complete.game.utils;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the raw play event info into PlayEvent objects. Each play event line has the following
 * format: username,timestamp_in_ms,readable_time,event_id
 * e.g.:
 * user2_AsparagusPig,AsparagusPig,10,1445230923951,
 * 2015-11-02 09:09:28.224,e8018d7d-18a6-4265-ba7e-55666b898b6f
 * The human-readable time string is not used here.
 */
public class ParsePlayEventFn extends DoFn<String, PlayEvent> {

  // Log and count parse errors.
  private static final Logger LOG = LoggerFactory.getLogger(ParsePlayEventFn.class);
  private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

  @ProcessElement
  public void processElement(ProcessContext c) {
    String[] components = c.element().split(",");
    try {
      String user = components[0].trim();
      Long timestamp = Long.parseLong(components[1].trim());
      String eventId = components[3].trim();
      PlayEvent play = new PlayEvent(user, timestamp, eventId);
      c.output(play);
    } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
      numParseErrors.inc();
      LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
    }
  }
}
