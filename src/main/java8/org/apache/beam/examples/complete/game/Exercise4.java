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

package org.apache.beam.examples.complete.game;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.examples.complete.game.solutions.Exercise1;
import org.apache.beam.examples.complete.game.solutions.Exercise3;
import org.apache.beam.examples.complete.game.utils.GameEvent;
import org.apache.beam.examples.complete.game.utils.Options;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Fourth in a series of coding exercises in a gaming domain.
 *
 * <p>This streaming pipeline calculates user and team scores for a window of time and writes them
 * to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise4 {

  static final Duration TEN_SECONDS = Duration.standardSeconds(10);
  static final Duration THIRTY_SECONDS = Duration.standardSeconds(30);

  /**
   * Exercise4Options supported by {@link Exercise4}.
   */
  interface Exercise4Options extends Options, StreamingOptions {

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(1)
    Integer getTeamWindowDuration();

    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(2)
    Integer getAllowedLateness();

    void setAllowedLateness(Integer value);
  }

  /**
   * Extract user/score pairs from the event stream using processing time, via global windowing. Get
   * periodic updates on all users' running scores.
   */
  @VisibleForTesting
  static class CalculateUserScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {

    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameEvent> input) {
      // [START EXERCISE 4 PART 1]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/par-do
      //
      // Fill in the code to:
      //   1. Window the incoming input into global windows
      //   2. trigger every thirty seconds to emit speculative results.
      return input
          /* TODO: SOLUTION CODE HERE */
          // Extract and sum username/score pairs from the event data.
          .apply("ExtractUserScore", new Exercise1.ExtractAndSumScore("user"));
      // [END EXERCISE 4 PART 1]:
    }
  }

  /**
   * Calculates scores for each team within the configured window duration.
   */
  // Extract team/score pairs from the event stream, using hour-long windows by default.
  @VisibleForTesting
  static class CalculateTeamScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {

    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameEvent> infos) {
      // [START EXERCISE 4 PART 2]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/par-do
      //
      // Fill in the code to:
      //   1. Window the incoming input into fixed windows of team window duration
      //   2. trigger on time results at the watermark
      //   3. trigger speculative results every ten seconds
      //   4. trigger late data results with a delay of thirty seconds
      return infos
          /* TODO: SOLUTION CODE HERE */
          // Extract and sum teamname/score pairs from the event data.
          .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"));
      // [END EXERCISE 4 PART 2]:
    }
  }

  public static void main(String[] args) throws Exception {
    Exercise4Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise4Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference teamTable = new TableReference();
    teamTable.setDatasetId(options.getOutputDataset());
    teamTable.setProjectId(options.as(GcpOptions.class).getProject());
    teamTable.setTableId(options.getOutputTableName() + "_team");

    TableReference userTable = new TableReference();
    userTable.setDatasetId(options.getOutputDataset());
    userTable.setProjectId(options.as(GcpOptions.class).getProject());
    userTable.setTableId(options.getOutputTableName() + "_user");

    PCollection<GameEvent> gameEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply("FormatTeamScores", ParDo.of(new FormatTeamScoreFn()))
        .apply(
            BigQueryIO.writeTableRows().to(teamTable)
                .withSchema(FormatTeamScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    gameEvents
        .apply(
            "CalculateUserScores",
            new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply("FormatUserScores", ParDo.of(new FormatUserScoreFn()))
        .apply(
            BigQueryIO.writeTableRows().to(userTable)
                .withSchema(FormatUserScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
  }

  /**
   * Format a KV of team and associated properties to a BigQuery TableRow.
   */
  protected static class FormatTeamScoreFn extends DoFn<KV<String, Integer>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow window) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", window.start().getMillis() / 1000)
              .set("processing_time", Instant.now().getMillis() / 1000)
              .set("timing", c.pane().getTiming().toString());
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * Format a KV of user and associated properties to a BigQuery TableRow.
   */
  static class FormatUserScoreFn extends DoFn<KV<String, Integer>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("user", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("processing_time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
