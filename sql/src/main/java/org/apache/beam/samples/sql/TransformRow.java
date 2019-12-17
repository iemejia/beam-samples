package org.apache.beam.samples.sql;

import java.security.InvalidParameterException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

/**
 * Chooses the event time from a column in the schema-ed object. Like WithTimestamp for schema-based
 * PCollections.
 */
public class TransformRow extends PTransform<PCollection<Row>, PCollection<Row>> {
  private String fieldName;

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    if (!input.hasSchema()) {
      throw new InvalidParameterException("input does not have schema");
    }
    Schema schema = input.getSchema();
//      FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(schema);
//      Schema outputSchema = SelectHelpers.getOutputSchema(schema, resolved);

    return input.apply(
        WithTimestamps.of(
            (SerializableFunction<Row, Instant>)
                row -> {
                  ReadableDateTime dateTime = row.getDateTime(fieldName);
                  return (dateTime == null) ? null : dateTime.toInstant();
                }));
  }

  private TransformRow(String fieldName) {
    this.fieldName = fieldName;
  }

  public static TransformRow withEventTime(String fieldName) {
    return new TransformRow(fieldName);
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

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

    PCollection<Row> people =
        p.apply(
            "Create_people",
            Create.of(person1, person2, person3, person4).withRowSchema(personSchema));
    people.apply(
        "log_people",
        ParDo.of(
            new DoFn<Row, Row>() {
              @DoFn.ProcessElement
              public void processElement(ProcessContext c) {
                System.out.println("people: " + c.element());
                System.out.println("people: " + c.timestamp());
                c.output(c.element());
              }
            }));

    PCollection<Row> shifted = people.apply(TransformRow.withEventTime("creation_time"));
    shifted.apply(
        "log_shifted",
        ParDo.of(
            new DoFn<Row, Row>() {
              @DoFn.ProcessElement
              public void processElement(ProcessContext c) {
                System.out.println("shifted: " + c.element());
                System.out.println("shifted: " + c.timestamp());
                c.output(c.element());
              }
            }));

    p.run().waitUntilFinish();
  }
}
