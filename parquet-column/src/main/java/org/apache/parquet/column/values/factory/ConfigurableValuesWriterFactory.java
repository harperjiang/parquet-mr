package org.apache.parquet.column.values.factory;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

public class ConfigurableValuesWriterFactory extends DefaultValuesWriterFactory {

  private ParquetProperties parquetProperties;

  private String getIntEncoding() {
    return System.getProperty("edu.uchicago.cs.encsel.intEnc");
  }

  private String getStringEncoding() {
    return System.getProperty("edu.uchicago.cs.encsel.stringEnc");
  }

  private int getIntBitLength() {
    return Integer.valueOf(System.getProperty("edu.uchicago.cs.encsel.intBitLen"));
  }

  private int getIntBound() {
    return Integer.valueOf(System.getProperty("edu.uchicago.cs.encsel.intBound"));
  }

  @Override
  public void initialize(ParquetProperties properties) {
    this.parquetProperties = properties;
  }

  private Encoding getEncodingForDataPage() {
    return RLE_DICTIONARY;
  }

  private Encoding getEncodingForDictionaryPage() {
    return PLAIN;
  }

  @Override
  public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
    switch (descriptor.getType()) {
      case BOOLEAN:
        return getBooleanValuesWriter();
      case FIXED_LEN_BYTE_ARRAY:
        return getFixedLenByteArrayValuesWriter(descriptor);
      case BINARY:
        return getBinaryValuesWriter(descriptor);
      case INT32:
        return getInt32ValuesWriter(descriptor);
      case INT64:
        return getInt64ValuesWriter(descriptor);
      case INT96:
        return getInt96ValuesWriter(descriptor);
      case DOUBLE:
        return getDoubleValuesWriter(descriptor);
      case FLOAT:
        return getFloatValuesWriter(descriptor);
      default:
        throw new IllegalArgumentException("Unknown type " + descriptor.getType());
    }
  }

  private ValuesWriter getBooleanValuesWriter() {
    // no dictionary encoding for boolean
    return new RunLengthBitPackingHybridValuesWriter(1, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
  }

  private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    switch (getStringEncoding()) {
      case "DELTAL":
        return new DeltaLengthByteArrayValuesWriter(parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      case "DELTA":
        return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      case "PLAIN":
        return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      default:
        return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }
  }

  private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path) {
    int intBitLength = getIntBitLength();
    int intBound = getIntBound();
    switch (getIntEncoding()) {
      case "BP":
        if (intBitLength <= 8) {
          return new BitPackingValuesWriter(intBound, parquetProperties.getInitialSlabSize(),
            parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
        } else {
          return new ByteBitPackingValuesWriter(intBound, Packer.BIG_ENDIAN);
        }
      case "DELTABP":
        return new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      case "RLE":
        return new RunLengthBitPackingHybridValuesWriter(intBitLength, parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      case "PLAIN":
        return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
          parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
      default:
        ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
        return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
    }
  }

  private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new DeltaBinaryPackingValuesWriterForLong(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new FixedLenByteArrayPlainValuesWriter(12, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }

  private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
    ValuesWriter fallbackWriter = new PlainValuesWriter(parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    return DefaultValuesWriterFactory.dictWriterWithFallBack(path, parquetProperties, getEncodingForDictionaryPage(), getEncodingForDataPage(), fallbackWriter);
  }
}
