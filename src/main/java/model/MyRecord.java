/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package model;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class MyRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3178178776532607146L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"MyRecord\",\"namespace\":\"model\",\"fields\":[{\"name\":\"str1\",\"type\":\"string\",\"doc\":\"String1\"},{\"name\":\"str2\",\"type\":\"string\",\"doc\":\"String2\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** String1 */
  @Deprecated public java.lang.CharSequence str1;
  /** String2 */
  @Deprecated public java.lang.CharSequence str2;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public MyRecord() {}

  /**
   * All-args constructor.
   * @param str1 String1
   * @param str2 String2
   */
  public MyRecord(java.lang.CharSequence str1, java.lang.CharSequence str2) {
    this.str1 = str1;
    this.str2 = str2;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return str1;
    case 1: return str2;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: str1 = (java.lang.CharSequence)value$; break;
    case 1: str2 = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'str1' field.
   * @return String1
   */
  public java.lang.CharSequence getStr1() {
    return str1;
  }

  /**
   * Sets the value of the 'str1' field.
   * String1
   * @param value the value to set.
   */
  public void setStr1(java.lang.CharSequence value) {
    this.str1 = value;
  }

  /**
   * Gets the value of the 'str2' field.
   * @return String2
   */
  public java.lang.CharSequence getStr2() {
    return str2;
  }

  /**
   * Sets the value of the 'str2' field.
   * String2
   * @param value the value to set.
   */
  public void setStr2(java.lang.CharSequence value) {
    this.str2 = value;
  }

  /**
   * Creates a new MyRecord RecordBuilder.
   * @return A new MyRecord RecordBuilder
   */
  public static model.MyRecord.Builder newBuilder() {
    return new model.MyRecord.Builder();
  }

  /**
   * Creates a new MyRecord RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new MyRecord RecordBuilder
   */
  public static model.MyRecord.Builder newBuilder(model.MyRecord.Builder other) {
    return new model.MyRecord.Builder(other);
  }

  /**
   * Creates a new MyRecord RecordBuilder by copying an existing MyRecord instance.
   * @param other The existing instance to copy.
   * @return A new MyRecord RecordBuilder
   */
  public static model.MyRecord.Builder newBuilder(model.MyRecord other) {
    return new model.MyRecord.Builder(other);
  }

  /**
   * RecordBuilder for MyRecord instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<MyRecord>
    implements org.apache.avro.data.RecordBuilder<MyRecord> {

    /** String1 */
    private java.lang.CharSequence str1;
    /** String2 */
    private java.lang.CharSequence str2;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(model.MyRecord.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.str1)) {
        this.str1 = data().deepCopy(fields()[0].schema(), other.str1);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.str2)) {
        this.str2 = data().deepCopy(fields()[1].schema(), other.str2);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing MyRecord instance
     * @param other The existing instance to copy.
     */
    private Builder(model.MyRecord other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.str1)) {
        this.str1 = data().deepCopy(fields()[0].schema(), other.str1);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.str2)) {
        this.str2 = data().deepCopy(fields()[1].schema(), other.str2);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'str1' field.
      * String1
      * @return The value.
      */
    public java.lang.CharSequence getStr1() {
      return str1;
    }

    /**
      * Sets the value of the 'str1' field.
      * String1
      * @param value The value of 'str1'.
      * @return This builder.
      */
    public model.MyRecord.Builder setStr1(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.str1 = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'str1' field has been set.
      * String1
      * @return True if the 'str1' field has been set, false otherwise.
      */
    public boolean hasStr1() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'str1' field.
      * String1
      * @return This builder.
      */
    public model.MyRecord.Builder clearStr1() {
      str1 = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'str2' field.
      * String2
      * @return The value.
      */
    public java.lang.CharSequence getStr2() {
      return str2;
    }

    /**
      * Sets the value of the 'str2' field.
      * String2
      * @param value The value of 'str2'.
      * @return This builder.
      */
    public model.MyRecord.Builder setStr2(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.str2 = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'str2' field has been set.
      * String2
      * @return True if the 'str2' field has been set, false otherwise.
      */
    public boolean hasStr2() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'str2' field.
      * String2
      * @return This builder.
      */
    public model.MyRecord.Builder clearStr2() {
      str2 = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public MyRecord build() {
      try {
        MyRecord record = new MyRecord();
        record.str1 = fieldSetFlags()[0] ? this.str1 : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.str2 = fieldSetFlags()[1] ? this.str2 : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
