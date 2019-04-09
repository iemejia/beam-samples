/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.samples.ingest;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestUsingFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(IngestUsingFileIO.class);
  /** Specific pipeline options. */
  private interface Options extends PipelineOptions {
    String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

    @Description("GDELT file date")
    @Default.InstanceFactory(Options.GDELTFileFactory.class)
    String getDate();

    void setDate(String value);

    @Description("Input Path")
    String getInput();

    void setInput(String value);

    @Description("Output Path")
    String getOutput();

    void setOutput(String value);

    class GDELTFileFactory implements DefaultValueFactory<String> {
      public String create(PipelineOptions options) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.format(new Date());
      }
    }
  }

  private static final String SCHEMA_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String[] SCIENTISTS =
      new String[] {
        "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
        "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
      };

  private static List<GenericRecord> generateGenericRecords(long count) {
    ArrayList<GenericRecord> data = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
    for (int i = 0; i < count; i++) {
      int index = i % SCIENTISTS.length;
      GenericRecord record = builder.set("name", SCIENTISTS[index]).build();
      data.add(record);
    }
    return data;
  }

  private static GenericRecord buildGenericRecord(final String name) {
    GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
    return builder.set("name", name).build();
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    if (options.getInput() == null) {
      options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
    }
    LOG.info(options.toString());

    GenericRecord x = buildGenericRecord("coco");
//    Class<? extends GenericRecord> recordClass = x.getClass();
    Class<GenericRecord> recordClass = (Class<GenericRecord>) x.getClass();

    Pipeline pipeline = Pipeline.create(options);
    PCollection<GenericRecord> pcol = pipeline
            .apply(Create.of(generateGenericRecords(1000)).withCoder(AvroCoder.of(SCHEMA)));

    pcol
        .apply(FileIO.<GenericRecord>write().via(AvroIO.sink(recordClass)).to(options.getOutput()));

//    pipeline
//        .apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
//        .apply("WriteToFS", TextIO.write().to(options.getOutput()));

    Create.of(1, 2, 3);
    pipeline.run();
  }
}
