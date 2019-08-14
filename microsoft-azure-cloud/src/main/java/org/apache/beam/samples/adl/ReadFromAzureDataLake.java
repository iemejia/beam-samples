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
package org.apache.beam.samples.adl;

import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example from the Beam source root with
 *
 * <pre>
 *   ./gradlew :sdks:java:extensions:sql:runBasicExample
 * </pre>
 *
 * <p>The above command executes the example locally using direct runner. Running the pipeline in
 * other runners require additional setup and are out of scope of the SQL examples. Please consult
 * Beam documentation on how to run pipelines.
 */
public class ReadFromAzureDataLake {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFromAzureDataLake.class);

  public interface Options extends HadoopFileSystemOptions {}

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Configuration config = new Configuration();
    options.setHdfsConfiguration(Collections.singletonList(config));

    Pipeline p = Pipeline.create(options);
    LOG.info(options.toString());
    PCollection<String> lines = p.apply(TextIO.read().from("hdfs://test/coco"));
    //    PCollection<String> lines = p.apply(TextIO.read().from("wasbs://test/coco"));
    lines.apply(
        MapElements.via(
            new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                System.out.println(input);
                return input;
              }
            }));

    p.run().waitUntilFinish();
  }
}
