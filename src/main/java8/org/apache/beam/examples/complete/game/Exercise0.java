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
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Zeroth (no code changes necessary) in a series of exercises in a gaming domain.
 *
 * <p>This batch pipeline imports game events from CSV to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise0 {

  /**
   * Format a GameEvent to a BigQuery TableRow.
   */
  static class FormatGameEventFn extends DoFn<GameEvent, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      GameEvent event = c.element();
      TableRow row = new TableRow()
          .set("user", event.getUser())
          .set("team", event.getTeam())
          .set("score", event.getScore())
          .set("timestamp", event.getTimestamp() / 1000);
      c.output(row);
    }

    /**
     * Defines the BigQuery schema.
     */
    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
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
    tableRef.setDatasetId(options.getOutputDataset());
    tableRef.setProjectId(options.as(GcpOptions.class).getProject());
    tableRef.setTableId(options.getOutputTableName());

    // Read events from a CSV file, parse them and write (import) them to BigQuery.
    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        .apply("FormatGameEvent", ParDo.of(new FormatGameEventFn()))
        .apply(
            BigQueryIO.<TableRow>write().to(tableRef)
                .withSchema(FormatGameEventFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}
