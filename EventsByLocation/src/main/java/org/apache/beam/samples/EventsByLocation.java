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
package org.apache.beam.samples;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.ConnectionFactory;

public class EventsByLocation {

    private static final Logger LOG = LoggerFactory.getLogger(EventsByLocation.class);
    /**
     * Specific pipeline options.
     */
    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(GDELTFileFactory.class)
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

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        if (options.getOutput() == null) {
            options.setOutput("/tmp/gdelt-" + options.getDate());
        }
        LOG.info(options.toString());

        String BROKER_URL = "tcp://localhost:61616";
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

        Pipeline pipeline = Pipeline.create(options);

//        PCollection<String> input =
//        pipeline
//                .apply("GDELTFile", TextIO.Read.from(options.getInput()));

        PCollection<JmsRecord> gdeltJmsCol = pipeline.apply(
                JmsIO.read()
                    .withConnectionFactory(connectionFactory)
                    .withQueue("gdelt")
//                    .withMaxNumRecords(3)
                    .withMaxNumRecords(Long.MAX_VALUE)
        );

        PCollection<String> input = gdeltJmsCol.apply("ExtractPayload", ParDo.of(new DoFn<JmsRecord, String>() {
            @Override
            public void processElement(ProcessContext c) throws Exception {
                JmsRecord jmsRecord = c.element();
                System.out.println(jmsRecord.getPayload());
                c.output(jmsRecord.getPayload());
            }
        }));

//        input.apply("gdelt",
//                        JmsIO.write().withConnectionFactory(connectionFactory).withQueue("gdelt"));
        input
                .apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
                    public void processElement(ProcessContext c) {
                        String[] fields = c.element().split("\\t+");
                        if (fields.length > 22) {
                            if (fields[21].length() > 2) {
                                c.output(fields[21].substring(0, 1));
                            } else {
                                c.output(fields[21]);
                            }
                        } else {
                            c.output("NA");
                        }
                    }
                }))
                .apply("Filtering", Filter.by(new SerializableFunction<String, Boolean>() {
                    public Boolean apply(String input) {
                        if (input.equals("NA")) {
                            return false;
                        }
                        if (input.startsWith("-")) {
                            return false;
                        }
                        if (input.length() != 2) {
                            return false;
                        }
                        return true;
                    }
                }))
//                .apply("CountPerLocation", Count.<String>perElement())
        Window.into(new IntervalWindow(Instant.now(), Instant.now().plus(1000)));
        Window.into()
                .apply("CountPerLocation", Count.<String>perElement())

                .apply("StringFormat", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))
                .apply("Results", TextIO.Write.to(options.getOutput()));
//                .apply("Results",
//                        JmsIO.write().withConnectionFactory(connectionFactory).withQueue("output"));
        pipeline.run();
    }

}
