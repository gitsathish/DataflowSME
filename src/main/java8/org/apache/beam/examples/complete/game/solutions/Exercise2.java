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
import org.apache.beam.examples.complete.game.utils.GameEvent;
import org.apache.beam.examples.complete.game.utils.Options;
import org.apache.beam.examples.complete.game.utils.ParseEventFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Second in a series of coding exercises in a gaming domain.
 *
 * <p>This batch pipeline calculates the sum of scores per team per hour, over an entire batch of
 * gaming data and writes the per-team sums to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise2 {

  /**
   * A transform to compute the WindowedTeamScore.
   */
  public static class WindowedTeamScore
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {

    private Duration duration;

    public WindowedTeamScore(Duration duration) {
      this.duration = duration;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameEvent> input) {
      return input
          .apply(Window.into(FixedWindows.of(duration)))
          .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"));
    }
  }

  /**
   * Format a KV of team and their score to a BigQuery TableRow.
   */
  public static class FormatTeamScoreSumsFn extends DoFn<KV<String, Integer>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow window) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", window.start().getMillis() / 1000);
      c.output(row);
    }

    /**
     * Defines the BigQuery schema.
     */
    public static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }

  /**
   * Run a batch pipeline.
   */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(options.as(Options.class).getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from a CSV file and parse them.
    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        .apply(
            "AddEventTimestamps", WithTimestamps.of((GameEvent i) -> new Instant(i.getTimestamp())))
        .apply("WindowedTeamScore", new WindowedTeamScore(Duration.standardMinutes(60)))
        // Write the results to BigQuery.
        .apply("FormatTeamScoreSums", ParDo.of(new FormatTeamScoreSumsFn()))
        .apply(
            BigQueryIO.writeTableRows().to(tableRef)
                .withSchema(FormatTeamScoreSumsFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
