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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.examples.complete.game.utils.GameEvent;
import org.apache.beam.examples.complete.game.utils.Options;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fifth in a series of coding exercises in a gaming domain.
 *
 * <p>This exercise introduces side inputs.
 *
 * <p>See README.md for details.
 */
public class Exercise5 {

  private static final Logger LOG = LoggerFactory.getLogger(Exercise5.class);

  /**
   * Filter out all but those users with a high clickrate, which we will consider as 'spammy' users.
   * We do this by finding the mean total score per user, then using that information as a side
   * input to filter out all but those user scores that are > (mean * SCORE_WEIGHT)
   */
  public static class CalculateSpammyUsers
      extends PTransform<PCollection<KV<String, Integer>>, PCollection<KV<String, Integer>>> {

    private static final Logger LOG = LoggerFactory.getLogger(CalculateSpammyUsers.class);
    private static final double SCORE_WEIGHT = 2.5;

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<KV<String, Integer>> userScores) {

      // Get the sum of scores for each user.
      PCollection<KV<String, Integer>> sumScores =
          userScores.apply("UserSum", Sum.<String>integersPerKey());

      // Extract the score from each element, and use it to find the global mean.
      final PCollectionView<Double> globalMeanScore =
          sumScores
              .apply(Values.<Integer>create())
              .apply(Mean.<Integer>globally().asSingletonView());

      // Filter the user sums using the global mean.
      PCollection<KV<String, Integer>> filtered =
          sumScores.apply("ProcessAndFilter",
              ParDo
                  // use the derived mean total score as a side input
                  .of(
                      new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                        private final Counter numSpammerUsers = Metrics
                            .counter("main", "SpammerUsers");

                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          Integer score = c.element().getValue();
                          Double gmc = c.sideInput(globalMeanScore);
                          if (score > (gmc * SCORE_WEIGHT)) {
                            LOG.info(
                                "user "
                                    + c.element().getKey()
                                    + " spammer score "
                                    + score
                                    + " with mean "
                                    + gmc);
                            numSpammerUsers.inc();
                            c.output(c.element());
                          }
                        }
                      })
                  .withSideInputs(globalMeanScore));
      return filtered;
    }
  }

  /**
   * Calculate and output an element's session duration.
   */
  private static class UserSessionInfoFn extends DoFn<KV<String, Integer>, Integer> {

    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow w) {
      int duration = new Duration(w.start(), w.end()).toPeriod().toStandardMinutes().getMinutes();
      c.output(duration);
    }
  }

  /**
   * Options supported by {@link Exercise5}.
   */
  interface Exercise5Options extends Options, StreamingOptions {

    @Description("Numeric value of fixed window duration for user analysis, in minutes")
    @Default.Integer(5)
    Integer getFixedWindowDuration();

    void setFixedWindowDuration(Integer value);
  }

  public static void main(String[] args) throws Exception {

    Exercise5Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise5Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference teamTable = new TableReference();
    teamTable.setDatasetId(options.getOutputDataset());
    teamTable.setProjectId(options.as(GcpOptions.class).getProject());
    teamTable.setTableId(options.getOutputTableName());

    PCollection<GameEvent> rawEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    // Extract username/score pairs from the event stream
    PCollection<KV<String, Integer>> userEvents =
        rawEvents.apply(
            "ExtractUserScore",
            MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                .via((GameEvent gInfo) -> KV.<String, Integer>of(gInfo.getUser(),
                    gInfo.getScore())));

    // Calculate the total score per user over fixed windows, and
    // cumulative updates for late data.
    final PCollectionView<Map<String, Integer>> spammersView =
        userEvents
            .apply("FixedWindowsUser",
                Window.<KV<String, Integer>>into(
                        FixedWindows.of(
                            Duration.standardMinutes(options.getFixedWindowDuration()))))

            // Filter out everyone but those with (SCORE_WEIGHT * avg) clickrate.
            // These might be robots/spammers.
            .apply("CalculateSpammyUsers", new CalculateSpammyUsers())
            // Derive a view from the collection of spammer users. It will be used as a side input
            // in calculating the team score sums, below.
            .apply("CreateSpammersView", View.<String, Integer>asMap());

    // Calculate the total score per team over fixed windows,
    // and emit cumulative updates for late data. Uses the side input derived above-- the set of
    // suspected robots-- to filter out scores from those users from the sum.
    // Write the results to BigQuery.
    rawEvents
        .apply("WindowIntoFixedWindows",
            Window
                .<GameEvent>into(
                    FixedWindows.of(Duration.standardMinutes(options.getFixedWindowDuration()))))
        // Filter out the detected spammer users, using the side input derived above.
        .apply("FilterOutSpammers",
            ParDo
                .of(
                    new DoFn<GameEvent, GameEvent>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        // If the user is not in the spammers Map, output the data element.
                        if (c.sideInput(spammersView).get(c.element().getUser().trim()) == null) {
                          c.output(c.element());
                        }
                      }
                    })
                .withSideInputs(spammersView))
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"))
        // Write the result to BigQuery
        .apply("FormatTeamWindows", ParDo.of(new FormatTeamWindowFn()))
        .apply(
            BigQueryIO.writeTableRows().to(teamTable)
                .withSchema(FormatTeamWindowFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
  }

  /**
   * Format a KV of team and associated properties to a BigQuery TableRow.
   */
  protected static class FormatTeamWindowFn extends DoFn<KV<String, Integer>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow window) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", window.start().getMillis() / 1000)
              .set("processing_time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
