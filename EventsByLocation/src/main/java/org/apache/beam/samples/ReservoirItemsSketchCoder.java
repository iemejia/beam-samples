package org.apache.beam.samples;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.datasketches.ArrayOfLongsSerDe;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;

public class ReservoirItemsSketchCoder extends CustomCoder<ReservoirItemsSketch> {

  // Avro's Schema class is not Serializable so we store it as a String
  private String schemaAsString;

  private ReservoirItemsSketchCoder(Schema schema) {
    this.schemaAsString = schema.toString();
  }

  public static ReservoirItemsSketchCoder of(Schema schema) {
    return new ReservoirItemsSketchCoder(schema);
  }

  @Override
  public void encode(ReservoirItemsSketch value, OutputStream outStream)
      throws CoderException, IOException {

    // TODO ugly double copy
    List<GenericRecord> records = new ArrayList<>();
    Object[] samples = value.getSamples();
    if (samples != null && samples.length > 0) {
      for (Object sample : samples) {
        records.add((GenericRecord) sample);
      }
    }

    VarIntCoder.of().encode(value.getK(), outStream); // reservoirSize_
    VarLongCoder.of().encode(value.getN(), outStream); // itemsSeen_
    // We assume that data will always be GenericRecord
    Parser parser = new Parser();
    Schema schema = parser.parse(schemaAsString);
    ListCoder.of(AvroGenericCoder.of(GenericRecord.class, schema)).encode(records, outStream);
  }

  @Override
  public ReservoirItemsSketch decode(InputStream inStream) throws CoderException, IOException {
    Integer k = VarIntCoder.of().decode(inStream); // reservoirSize_
    Long n = VarLongCoder.of().decode(inStream); // itemsSeen_
    Parser parser = new Parser();
    Schema schema = parser.parse(schemaAsString);
    List<GenericRecord> records = ListCoder.of(AvroGenericCoder.of(GenericRecord.class, schema)).decode(inStream);
    ReservoirItemsSketch value = ReservoirItemsSketch.newInstance(k);
    ReservoirItemsUnion<GenericRecord> union = ReservoirItemsUnion.newInstance(k);
    union.update(n, k, (ArrayList<GenericRecord>) records);
    ReservoirItemsSketch<GenericRecord> result = union.getResult();
    return result;
  }


  private static void validateSerializeAndDeserialize(final ReservoirItemsSketch<Long> ris) {
    final byte[] sketchBytes = ris.toByteArray(new ArrayOfLongsSerDe());
//    assertEquals(sketchBytes.length,
//            (Family.RESERVOIR.getMaxPreLongs() + ris.getNumSamples()) << 3);
    // ensure full reservoir rebuilds correctly
    final Memory mem = Memory.wrap(sketchBytes);
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    final ReservoirItemsSketch<Long> loadedRis = ReservoirItemsSketch.heapify(mem, serDe);
//    validateReservoirEquality(ris, loadedRis);
  }

  public static void main(String[] args) {
    final String SCHEMA_STRING =
            "{\"namespace\": \"example.avro\",\n"
                    + " \"type\": \"record\",\n"
                    + " \"name\": \"Image\",\n"
                    + " \"fields\": [\n"
                    + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                    + "     {\"name\": \"width\", \"type\": \"int\"},\n"
                    + "     {\"name\": \"height\", \"type\": \"int\"}\n"
                    + " ]\n"
                    + "}";
    final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("name", "image");
    record.put("width", 1200);
    record.put("height", 1000);

    ReservoirItemsSketch<GenericRecord> value = ReservoirItemsSketch.newInstance(100);
    value.update(record);

    ReservoirItemsSketchCoder coder = ReservoirItemsSketchCoder.of(SCHEMA);

    try {
      CoderProperties.structuralValueDecodeEncodeEqual(coder, value);
      System.out.println("it works");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
