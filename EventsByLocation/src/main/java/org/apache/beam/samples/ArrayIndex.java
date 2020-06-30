package org.apache.beam.samples;

import org.apache.avro.generic.IndexedRecord;
import org.apache.datasketches.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Memory;

public class ArrayIndex extends ArrayOfItemsSerDe<IndexedRecord> {
    @Override
    public byte[] serializeToByteArray(IndexedRecord[] items) {
        return new byte[0];
    }

    @Override
    public IndexedRecord[] deserializeFromMemory(Memory mem, int numItems) {
        return new IndexedRecord[0];
    }
}
