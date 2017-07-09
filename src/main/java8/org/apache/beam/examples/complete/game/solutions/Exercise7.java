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
package org.apache.beam.examples.complete.game.solutions;

import static org.apache.beam.sdk.transforms.windowing.TimestampCombiner.END_OF_WINDOW;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.examples.complete.game.utils.GameEvent;
import org.apache.beam.examples.complete.game.utils.Options;
import org.apache.beam.examples.complete.game.utils.ParseEventFn;
import org.apache.beam.examples.complete.game.utils.ParsePlayEventFn;
import org.apache.beam.examples.complete.game.utils.PlayEvent;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ApproximateQuantiles;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seventh in a series of coding exercises in a gaming domain.
 *
 * <p>This exercise introduces concepts of pubsub message ids, session windows, side inputs, joins,
 * and global combine.
 *
 * <p>See README.md for details.
 */
public class Exercise7 {

  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";
  private static final String MESSAGE_ID_ATTRIBUTE = "unique_id";
  private static final int SESSION_GAP_MINUTES = 1;
  private static final int GLOBAL_LATENCY_QUANTILES = 31;
  private static final int GLOBAL_AGGREGATE_FANOUT = 16;
  private static final Logger LOG = LoggerFactory.getLogger(Exercise7.class);

  private static final TupleTag<PlayEvent> playTag = new TupleTag<PlayEvent>();
  private static final TupleTag<GameEvent> eventTag = new TupleTag<GameEvent>();

  /**
   * Options supported by {@link Exercise7}.
   */
  interface Exercise7Options extends Options, StreamingOptions {

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();

    void setTopic(String value);

    @Description("Pub/Sub play events topic to read from")
    @Validation.Required
    String getPlayEventsTopic();

    void setPlayEventsTopic(String value);
  }

  public static class ComputeLatencyFn extends DoFn<KV<String, CoGbkResult>, KV<String, Long>> {

    private final Counter numDroppedSessionsNoEvent = Metrics
                            .counter("main", "DroppedSessionsNoEvent");
    private final Counter numDroppedSessionsTooManyEvents = Metrics
        .counter("main", "DroppedSessionsTooManyEvents");
    private final Counter numDroppedSessionsNoPlayEvents = Metrics
        .counter("main", "DroppedSessionsNoPlayEvents");

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<PlayEvent> plays = c.element().getValue().getAll(playTag);
      Iterable<GameEvent> events = c.element().getValue().getAll(eventTag);
      int playCount = 0;
      Long maxPlayEventTs = 0L;
      for (PlayEvent play : plays) {
        playCount++;
        maxPlayEventTs = Math.max(maxPlayEventTs, play.getTimestamp());
      }
      int eventCount = 0;
      GameEvent event = null;
      for (GameEvent currentEvent : events) {
        eventCount++;
        event = currentEvent;
      }
      String id = c.element().getKey();
      if (eventCount == 0) {
        numDroppedSessionsNoEvent.inc();
      } else if (eventCount > 1) {
        numDroppedSessionsTooManyEvents.inc();
      } else if (playCount == 0) {
        numDroppedSessionsNoPlayEvents.inc();
      } else {
        Long minLatency = event.getTimestamp() - maxPlayEventTs;
        c.output(KV.of(event.getUser(), minLatency));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Exercise7Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise7Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference badUserTable = new TableReference();
    badUserTable.setDatasetId(options.getOutputDataset());
    badUserTable.setProjectId(options.as(GcpOptions.class).getProject());
    badUserTable.setTableId(options.getOutputTableName() + "_bad_users");

    // Read Events from Pub/Sub using custom timestamps and custom message id label.
    PCollection<KV<String, GameEvent>> sessionedEvents =
        pipeline
            .apply(
                "ReadGameScoreEvents",
                PubsubIO.readStrings().withTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                    .withIdAttribute(MESSAGE_ID_ATTRIBUTE).fromTopic(options.getTopic()))
            .apply("ParseGameScoreEvents", ParDo.of(new ParseEventFn()))
            .apply(
                "KeyGameScoreByEventId",
                WithKeys.of((GameEvent event) -> event.getEventId())
                    .withKeyType(TypeDescriptor.of(String.class)))
            .apply(
                "SessionizeGameScoreEvents",
                Window.<KV<String, GameEvent>>into(
                    Sessions.withGapDuration(Duration.standardMinutes(SESSION_GAP_MINUTES)))
                    .withTimestampCombiner(END_OF_WINDOW));

    // Read PlayEvents from Pub/Sub using custom timestamps and custom message id label.
    PCollection<KV<String, PlayEvent>> sessionedPlayEvents =
        pipeline
            .apply(
                "ReadGamePlayEvents",
                PubsubIO.readStrings().withTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                    .withIdAttribute(MESSAGE_ID_ATTRIBUTE)
                    .fromTopic(options.getPlayEventsTopic()))
            .apply("ParseGamePlayEvents", ParDo.of(new ParsePlayEventFn()))
            .apply(
                "KeyGamePlayByEventId",
                WithKeys.of((PlayEvent play) -> play.getEventId())
                    .withKeyType(TypeDescriptor.of(String.class)))
            .apply(
                "SessionizeGamePlayEvents",
                Window.<KV<String, PlayEvent>>into(
                    Sessions.withGapDuration(Duration.standardMinutes(SESSION_GAP_MINUTES)))
                    .withTimestampCombiner(END_OF_WINDOW));

    // Compute per-user latency.
    PCollection<KV<String, Long>> userLatency =
        KeyedPCollectionTuple.of(playTag, sessionedPlayEvents)
            .and(eventTag, sessionedEvents)
            .apply("JoinScorePlayEvents", CoGroupByKey.create())
            .apply("ComputeLatency", ParDo.of(new ComputeLatencyFn()));

    // Create a view onto quantiles of the global latency distribution.
    PCollectionView<List<Long>> globalQuantiles =
        userLatency
            .apply("GetLatencies", Values.create())
            // Re-window session results into a global window, and trigger periodically making sure
            // to use the full accumulated window contents.
            .apply(
                "GlobalWindowRetrigger",
                Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1000)))
                    .accumulatingFiredPanes())
            .apply(
                ((Combine.Globally<Long, List<Long>>)
                    ApproximateQuantiles.<Long>globally(GLOBAL_LATENCY_QUANTILES))
                    .withFanout(GLOBAL_AGGREGATE_FANOUT)
                    .asSingletonView());

    userLatency
        // Use the computed latency distribution as a side-input to filter out likely bad users.
        .apply(
            "DetectBadUsers",
            ParDo
                .of(
                    new DoFn<KV<String, Long>, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String user = c.element().getKey();
                        Long latency = c.element().getValue();
                        List<Long> quantiles = c.sideInput(globalQuantiles);
                        // Users in the first quantile are considered spammers, since their
                        // score to play event latency is too low, suggesting a robot.
                        if (latency < quantiles.get(1)) {
                          c.output(user);
                        }
                      }
                    }).withSideInputs(globalQuantiles))
        // We want to only emit a single BigQuery row for every bad user. To do this, we
        // re-key by user, then window globally and trigger on the first element for each key.
        .apply(
            "KeyByUser",
            WithKeys.of((String user) -> user).withKeyType(TypeDescriptor.of(String.class)))
        .apply(
            "GlobalWindowsTriggerOnFirst",
            Window.<KV<String, String>>into(new GlobalWindows())
                .triggering(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(10)))
                .accumulatingFiredPanes())
        .apply("GroupByUser", GroupByKey.<String, String>create())
        .apply("FormatBadUsers", ParDo.of(new FormatBadUserFn()))
        .apply(
            "WriteBadUsers",
            BigQueryIO.writeTableRows().to(badUserTable)
                .withSchema(FormatBadUserFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
  }

  /**
   * Format a KV of user and associated properties to a BigQuery TableRow.
   */
  static class FormatBadUserFn extends DoFn<KV<String, Iterable<String>>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("bad_user", c.element().getKey())
              .set("time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("bad_user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
