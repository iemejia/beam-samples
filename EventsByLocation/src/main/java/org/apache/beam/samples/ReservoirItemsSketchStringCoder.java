package org.apache.beam.samples;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.datasketches.sampling.ReservoirItemsUnion;

//import org.apache.avro.generic.String;

public class ReservoirItemsSketchStringCoder extends CustomCoder<ReservoirItemsSketch> {
//  private static final ReservoirItemsSketchCoder INSTANCE = new ReservoirItemsSketchCoder();
  private transient Schema schema;

  private ReservoirItemsSketchStringCoder(Schema schema) {
    this.schema = schema;
  }

  public static ReservoirItemsSketchStringCoder of(Schema schema) {
    return new ReservoirItemsSketchStringCoder(schema);
  }

  @Override
  public void encode(ReservoirItemsSketch value, OutputStream outStream)
      throws CoderException, IOException {

    // TODO ugly double copy
    List<String> records = new ArrayList<>();
    Object[] samples = value.getSamples();
    if (samples != null && samples.length > 0) {
      for (Object sample : samples) {
        records.add((String) sample);
      }
    }

    VarIntCoder.of().encode(value.getK(), outStream); // reservoirSize_
    VarLongCoder.of().encode(value.getN(), outStream); // itemsSeen_
    // We assume that data will always be String
//    ListCoder.of(AvroCoder.of(String.class, schema)).encode(records, outStream);
    ListCoder.of(StringUtf8Coder.of()).encode(records, outStream);
  }

  @Override
  public ReservoirItemsSketch decode(InputStream inStream) throws CoderException, IOException {
    Integer k = VarIntCoder.of().decode(inStream); // reservoirSize_
    Long n = VarLongCoder.of().decode(inStream); // itemsSeen_
//    List<String> records = ListCoder.of(AvroCoder.of(String.class)).decode(inStream);
    List<String> records = ListCoder.of(StringUtf8Coder.of()).decode(inStream);
    ReservoirItemsSketch value = ReservoirItemsSketch.newInstance(k);
    ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(k);
    union.update(n, k, (ArrayList<String>) records);
    ReservoirItemsSketch<String> result = union.getResult();
    return result;
  }

  public static void main(String[] args) {
    Schema basicSchema = SchemaBuilder.record("basic").fields().name("g1").type().stringType().noDefault()
            .name("g2").type().stringType().noDefault().name("int1").type().unionOf().intType().and().nullType().endUnion()
            .noDefault().name("long1").type().unionOf().longType().and().nullType().endUnion().noDefault().name("float1").type()
            .unionOf().floatType().and().nullType().endUnion().noDefault().name("double1").type().unionOf().doubleType().and()
            .nullType().endUnion().noDefault().name("array1").type().unionOf().array().items().stringType().and().nullType()
            .endUnion().noDefault().endRecord();
    ReservoirItemsSketch<String> value = ReservoirItemsSketch.newInstance(100);
    try {
      CoderProperties.structuralValueDecodeEncodeEqual(ReservoirItemsSketchStringCoder.of(basicSchema), value);
      System.out.println("it works");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
