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
package org.apache.beam.samples.sql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.transforms.Filter;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.schemas.transforms.RenameFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

/** Schema-based PTransform examples. */
class SchemaBasedPTransformsExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // define the input row format
    Schema personSchema =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addInt32Field("country")
            .addDoubleField("score")
            .addDateTimeField("creation_time")
            .build();

    Row person1 = Row.withSchema(personSchema).addValues(1, "Tom", 1, 5.0, new DateTime()).build();
    Row person2 = Row.withSchema(personSchema).addValues(2, "Sam", 33, 4.0, new DateTime()).build();
    Row person3 = Row.withSchema(personSchema).addValues(3, "Max", 39, 4.0, new DateTime()).build();
    Row person4 = Row.withSchema(personSchema).addValues(4, "Tim", 1, 3.0, new DateTime()).build();

    Schema countrySchema = Schema.builder().addInt32Field("country").addStringField("name").build();
    Row country1 = Row.withSchema(countrySchema).addValues(1, "USA").build();
    Row country2 = Row.withSchema(countrySchema).addValues(33, "France").build();
    Row country3 = Row.withSchema(countrySchema).addValues(39, "Italy").build();

    PCollection<Row> people =
        p.apply("Create_people", Create.of(person1, person2, person3, person4).withRowSchema(personSchema));

    PCollection<Row> countries =
        p.apply("Create_countries", Create.of(country1, country2, country3).withRowSchema(countrySchema));

    // weird error if the schema is the same for both pcollections on join

    PCollection<Row> selectFields = people.apply(Select.fieldNames("name"));
    selectFields.apply(
        "log_selectFields",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("selectFields: " + input.getValues());
                return input;
              }
            }));

    PCollection<Row> droppedFields = people.apply(DropFields.fields("name"));
    droppedFields.apply(
        "log_droppedFields",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("droppedFields: " + input.getValues());
                return input;
              }
            }));

    PCollection<Row> addedFields =
        people.apply(AddFields.<Row>create().field("c4", Schema.FieldType.STRING));
    addedFields.apply(
        "log_addedFields",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("addedFields: " + input.getValues());
                return input;
              }
            }));

    PCollection<Row> renamedFields =
        addedFields.apply(RenameFields.<Row>create().rename("c4", "city"));
    renamedFields.apply(
        "log_renamedFields",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("renamedFields: " + input.getValues());
                return input;
              }
            }));

    PCollection<Row> filteredElements =
        people.apply(
            Filter.<Row>create().whereFieldName("country", (Integer country) -> country == 1));
    selectFields.apply(
        "log_filteredElements",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("filteredElements: " + input.getValues());
                return input;
              }
            }));

    PCollection<Row> groupedElements = people.apply(Group.byFieldNames("country"));
    groupedElements.apply(
        "log_groupedElements",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("groupedElements: " + input.getValues());
                return input;
              }
            }));

    // TODO Cast, CoGroup, Convert, Unnest
    // TODO a transform to select a column as the new event time
    //      Join.innerJoin(countries).using("country");
    //      people.apply();
    // weird error on not using condition     //      .using("country")

    PCollection<Row> joinedElements =
        people.apply(Join.<Row, Row>innerJoin(countries).using("country"));
    groupedElements.apply(
        "log_joinedElements",
        MapElements.via(
            new SimpleFunction<Row, Row>() {
              @Override
              public Row apply(Row input) {
                System.out.println("joinedElements: " + input.getValues());
                return input;
              }
            }));

    p.run().waitUntilFinish();
  }
}
