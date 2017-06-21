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

import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.examples.complete.game.utils.GameEvent;
import org.apache.beam.examples.complete.game.utils.Options;
import org.apache.beam.examples.complete.game.utils.ParseEventFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Third in a series of coding exercises in a gaming domain.
 *
 * <p>This is the same pipeline as in Exercise 2, but can run in either batch or streaming mode.
 *
 * <p>See README.md for details.
 */
public class Exercise3 {

  /**
   * A transform to read the game events from either text files or Pub/Sub topic.
   */
  public static class ReadGameEvents extends PTransform<PBegin, PCollection<GameEvent>> {

    private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

    private Options options;

    public ReadGameEvents(Options options) {
      this.options = options;
    }

    @Override
    public PCollection<GameEvent> expand(PBegin begin) {
      if (options.getInput() != null && !options.getInput().isEmpty()) {
        return begin
            .getPipeline()
            .apply(TextIO.read().from(options.getInput()))
            .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
            .apply(
                "AddEventTimestamps",
                WithTimestamps.of((GameEvent i) -> new Instant(i.getTimestamp())));
      } else {
        return begin
            .getPipeline()
            .apply(PubsubIO.readStrings().withTimestampAttribute(TIMESTAMP_ATTRIBUTE)
                .fromTopic(options.getTopic()))
            .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));
      }
    }
  }

  /**
   * Run a batch or streaming pipeline.
   */
  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(options.as(Options.class).getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from either a CSV file or PubSub stream.
    pipeline
        .apply(new ReadGameEvents(options))
        .apply("WindowedTeamScore", new Exercise2.WindowedTeamScore(Duration.standardMinutes(60)))
        // Write the results to BigQuery.
        .apply("FormatTeamScoreSums", ParDo.of(new Exercise2.FormatTeamScoreSumsFn()))
        .apply(
            BigQueryIO.writeTableRows().to(tableRef)
                .withSchema(Exercise2.FormatTeamScoreSumsFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
