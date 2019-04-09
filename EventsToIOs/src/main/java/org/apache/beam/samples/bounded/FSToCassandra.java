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
package org.apache.beam.samples.bounded;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.samples.ingest.IngestToHBase;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.cassandra.CassandraServiceImpl;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FSToCassandra {

  private static final Logger LOG = LoggerFactory.getLogger(IngestToHBase.class);
  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {
    @Description("Input Path")
    String getInput();
    void setInput(String value);

    @Description("Output Path")
    String getOutput();
    void setOutput(String value);
  }

  private static String getCountry(String row) {
    String[] fields = row.split("\\t+");
    if (fields.length > 22) {
      if (fields[21].length() > 2) {
        return fields[21].substring(0, 1);
      }
      return fields[21];
    }
    return "NA";
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    LOG.info(options.toString());

    final Configuration conf = HBaseConfiguration.create();

//    CassandraIO.read().withCassandraService(new CassandraServiceImpl<>());

    int i = 1;
    List<String> cassandraHosts = Arrays.asList("localhost");
    int cassandraPort = 66212;

    Pipeline pipeline = Pipeline.create(options);
    pipeline
            .apply(Create.of(new Scientist(i, String.valueOf(i))))
        .apply(
            "WriteToCassandra",
            CassandraIO.<Scientist>write()
                    .withHosts(cassandraHosts)
                    .withPort(cassandraPort)
                    .withKeyspace(KEYSPACE)
                    .withEntity(Scientist.class));

//    pipeline
//        .apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
//        .apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
//      @ProcessElement
//      public void processElement(ProcessContext c) {
//        c.output(getCountry(c.element()));
//      }
//    }))
//            .apply("FilterValidLocations", Filter.by(new SerializableFunction<String, Boolean>() {
//              public Boolean apply(String input) {
//                return (!input.equals("NA") && !input.startsWith("-") && input.length() == 2);
//              }
//            }))
//            .apply("CountByLocation", Count.<String>perElement())
//            .apply("ConvertToJson", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//              public String apply(KV<String, Long> input) {
//                return "{\"" + input.getKey() + "\": " + input.getValue() + "}";
//              }
//            }))
//            .apply("WriteResults", TextIO.write().to(options.getOutput()));


//        .apply("ConvertToKV", MapElements.via(new SimpleFunction<String, KV<String, String>>() {
//          @Override
//          public KV<String, String> apply(String input) {
//            String key = "";
//            //TODO
//            return KV.of(key, input);
//          }
//        }));

//        .apply("ToHBaseMutation", MapElements.via(new SimpleFunction<KV<String, String>, Mutation>() {
//          @Override
//          public Mutation apply(KV<String, String> input) {
//            return makeMutation(input.getKey(), input.getValue());
//          }
//        }))
//        .apply(HBaseIO.write().withConfiguration(conf).withTableId(options.getInput()));

    pipeline.run();
  }

  private static final String KEYSPACE = "BEAM";
  private static final String TABLE = "BEAM_TEST";

  /** Simple Cassandra entity used in test. */
  @Table(name = TABLE, keyspace = KEYSPACE)
  private static final class Scientist implements Serializable {
    @PartitionKey
    @Column(name = "id")
    final long id;

    @Column(name = "name")
    final String name;

    Scientist() {
      // Empty constructor needed for deserialization from Cassandra
      this(0, null);
    }

    Scientist(long id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public String toString() {
      return id + ": " + name;
    }
  }
}
