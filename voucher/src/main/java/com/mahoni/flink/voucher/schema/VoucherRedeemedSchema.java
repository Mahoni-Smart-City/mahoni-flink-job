/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.mahoni.flink.voucher.schema;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@org.apache.avro.specific.AvroGenerated
public class VoucherRedeemedSchema extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -4080736498484615883L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"VoucherRedeemedSchema\",\"namespace\":\"com.mahoni.schema\",\"fields\":[{\"name\":\"eventId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"voucherId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"code\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"point\",\"type\":\"int\"},{\"name\":\"redeemedAt\",\"type\":\"long\"},{\"name\":\"expiredAt\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<VoucherRedeemedSchema> ENCODER =
      new BinaryMessageEncoder<VoucherRedeemedSchema>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<VoucherRedeemedSchema> DECODER =
      new BinaryMessageDecoder<VoucherRedeemedSchema>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<VoucherRedeemedSchema> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<VoucherRedeemedSchema> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<VoucherRedeemedSchema> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<VoucherRedeemedSchema>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this VoucherRedeemedSchema to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a VoucherRedeemedSchema from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a VoucherRedeemedSchema instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static VoucherRedeemedSchema fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private String eventId;
   private long timestamp;
   private String voucherId;
   private String userId;
   private String code;
   private int point;
   private long redeemedAt;
   private long expiredAt;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public VoucherRedeemedSchema() {}

  /**
   * All-args constructor.
   * @param eventId The new value for eventId
   * @param timestamp The new value for timestamp
   * @param voucherId The new value for voucherId
   * @param userId The new value for userId
   * @param code The new value for code
   * @param point The new value for point
   * @param redeemedAt The new value for redeemedAt
   * @param expiredAt The new value for expiredAt
   */
  public VoucherRedeemedSchema(String eventId, Long timestamp, String voucherId, String userId, String code, Integer point, Long redeemedAt, Long expiredAt) {
    this.eventId = eventId;
    this.timestamp = timestamp;
    this.voucherId = voucherId;
    this.userId = userId;
    this.code = code;
    this.point = point;
    this.redeemedAt = redeemedAt;
    this.expiredAt = expiredAt;
  }

  public SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return eventId;
    case 1: return timestamp;
    case 2: return voucherId;
    case 3: return userId;
    case 4: return code;
    case 5: return point;
    case 6: return redeemedAt;
    case 7: return expiredAt;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: eventId = value$ != null ? value$.toString() : null; break;
    case 1: timestamp = (Long)value$; break;
    case 2: voucherId = value$ != null ? value$.toString() : null; break;
    case 3: userId = value$ != null ? value$.toString() : null; break;
    case 4: code = value$ != null ? value$.toString() : null; break;
    case 5: point = (Integer)value$; break;
    case 6: redeemedAt = (Long)value$; break;
    case 7: expiredAt = (Long)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'eventId' field.
   * @return The value of the 'eventId' field.
   */
  public String getEventId() {
    return eventId;
  }


  /**
   * Sets the value of the 'eventId' field.
   * @param value the value to set.
   */
  public void setEventId(String value) {
    this.eventId = value;
  }

  /**
   * Gets the value of the 'timestamp' field.
   * @return The value of the 'timestamp' field.
   */
  public long getTimestamp() {
    return timestamp;
  }


  /**
   * Sets the value of the 'timestamp' field.
   * @param value the value to set.
   */
  public void setTimestamp(long value) {
    this.timestamp = value;
  }

  /**
   * Gets the value of the 'voucherId' field.
   * @return The value of the 'voucherId' field.
   */
  public String getVoucherId() {
    return voucherId;
  }


  /**
   * Sets the value of the 'voucherId' field.
   * @param value the value to set.
   */
  public void setVoucherId(String value) {
    this.voucherId = value;
  }

  /**
   * Gets the value of the 'userId' field.
   * @return The value of the 'userId' field.
   */
  public String getUserId() {
    return userId;
  }


  /**
   * Sets the value of the 'userId' field.
   * @param value the value to set.
   */
  public void setUserId(String value) {
    this.userId = value;
  }

  /**
   * Gets the value of the 'code' field.
   * @return The value of the 'code' field.
   */
  public String getCode() {
    return code;
  }


  /**
   * Sets the value of the 'code' field.
   * @param value the value to set.
   */
  public void setCode(String value) {
    this.code = value;
  }

  /**
   * Gets the value of the 'point' field.
   * @return The value of the 'point' field.
   */
  public int getPoint() {
    return point;
  }


  /**
   * Sets the value of the 'point' field.
   * @param value the value to set.
   */
  public void setPoint(int value) {
    this.point = value;
  }

  /**
   * Gets the value of the 'redeemedAt' field.
   * @return The value of the 'redeemedAt' field.
   */
  public long getRedeemedAt() {
    return redeemedAt;
  }


  /**
   * Sets the value of the 'redeemedAt' field.
   * @param value the value to set.
   */
  public void setRedeemedAt(long value) {
    this.redeemedAt = value;
  }

  /**
   * Gets the value of the 'expiredAt' field.
   * @return The value of the 'expiredAt' field.
   */
  public long getExpiredAt() {
    return expiredAt;
  }


  /**
   * Sets the value of the 'expiredAt' field.
   * @param value the value to set.
   */
  public void setExpiredAt(long value) {
    this.expiredAt = value;
  }

  /**
   * Creates a new VoucherRedeemedSchema RecordBuilder.
   * @return A new VoucherRedeemedSchema RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new VoucherRedeemedSchema RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new VoucherRedeemedSchema RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * Creates a new VoucherRedeemedSchema RecordBuilder by copying an existing VoucherRedeemedSchema instance.
   * @param other The existing instance to copy.
   * @return A new VoucherRedeemedSchema RecordBuilder
   */
  public static Builder newBuilder(VoucherRedeemedSchema other) {
    if (other == null) {
      return new Builder();
    } else {
      return new Builder(other);
    }
  }

  /**
   * RecordBuilder for VoucherRedeemedSchema instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<VoucherRedeemedSchema>
    implements org.apache.avro.data.RecordBuilder<VoucherRedeemedSchema> {

    private String eventId;
    private long timestamp;
    private String voucherId;
    private String userId;
    private String code;
    private int point;
    private long redeemedAt;
    private long expiredAt;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.eventId)) {
        this.eventId = data().deepCopy(fields()[0].schema(), other.eventId);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[1].schema(), other.timestamp);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.voucherId)) {
        this.voucherId = data().deepCopy(fields()[2].schema(), other.voucherId);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.userId)) {
        this.userId = data().deepCopy(fields()[3].schema(), other.userId);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.code)) {
        this.code = data().deepCopy(fields()[4].schema(), other.code);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.point)) {
        this.point = data().deepCopy(fields()[5].schema(), other.point);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
      if (isValidValue(fields()[6], other.redeemedAt)) {
        this.redeemedAt = data().deepCopy(fields()[6].schema(), other.redeemedAt);
        fieldSetFlags()[6] = other.fieldSetFlags()[6];
      }
      if (isValidValue(fields()[7], other.expiredAt)) {
        this.expiredAt = data().deepCopy(fields()[7].schema(), other.expiredAt);
        fieldSetFlags()[7] = other.fieldSetFlags()[7];
      }
    }

    /**
     * Creates a Builder by copying an existing VoucherRedeemedSchema instance
     * @param other The existing instance to copy.
     */
    private Builder(VoucherRedeemedSchema other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.eventId)) {
        this.eventId = data().deepCopy(fields()[0].schema(), other.eventId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[1].schema(), other.timestamp);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.voucherId)) {
        this.voucherId = data().deepCopy(fields()[2].schema(), other.voucherId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.userId)) {
        this.userId = data().deepCopy(fields()[3].schema(), other.userId);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.code)) {
        this.code = data().deepCopy(fields()[4].schema(), other.code);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.point)) {
        this.point = data().deepCopy(fields()[5].schema(), other.point);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.redeemedAt)) {
        this.redeemedAt = data().deepCopy(fields()[6].schema(), other.redeemedAt);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.expiredAt)) {
        this.expiredAt = data().deepCopy(fields()[7].schema(), other.expiredAt);
        fieldSetFlags()[7] = true;
      }
    }

    /**
      * Gets the value of the 'eventId' field.
      * @return The value.
      */
    public String getEventId() {
      return eventId;
    }


    /**
      * Sets the value of the 'eventId' field.
      * @param value The value of 'eventId'.
      * @return This builder.
      */
    public Builder setEventId(String value) {
      validate(fields()[0], value);
      this.eventId = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'eventId' field has been set.
      * @return True if the 'eventId' field has been set, false otherwise.
      */
    public boolean hasEventId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'eventId' field.
      * @return This builder.
      */
    public Builder clearEventId() {
      eventId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestamp' field.
      * @return The value.
      */
    public long getTimestamp() {
      return timestamp;
    }


    /**
      * Sets the value of the 'timestamp' field.
      * @param value The value of 'timestamp'.
      * @return This builder.
      */
    public Builder setTimestamp(long value) {
      validate(fields()[1], value);
      this.timestamp = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'timestamp' field has been set.
      * @return True if the 'timestamp' field has been set, false otherwise.
      */
    public boolean hasTimestamp() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'timestamp' field.
      * @return This builder.
      */
    public Builder clearTimestamp() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'voucherId' field.
      * @return The value.
      */
    public String getVoucherId() {
      return voucherId;
    }


    /**
      * Sets the value of the 'voucherId' field.
      * @param value The value of 'voucherId'.
      * @return This builder.
      */
    public Builder setVoucherId(String value) {
      validate(fields()[2], value);
      this.voucherId = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'voucherId' field has been set.
      * @return True if the 'voucherId' field has been set, false otherwise.
      */
    public boolean hasVoucherId() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'voucherId' field.
      * @return This builder.
      */
    public Builder clearVoucherId() {
      voucherId = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'userId' field.
      * @return The value.
      */
    public String getUserId() {
      return userId;
    }


    /**
      * Sets the value of the 'userId' field.
      * @param value The value of 'userId'.
      * @return This builder.
      */
    public Builder setUserId(String value) {
      validate(fields()[3], value);
      this.userId = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'userId' field has been set.
      * @return True if the 'userId' field has been set, false otherwise.
      */
    public boolean hasUserId() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'userId' field.
      * @return This builder.
      */
    public Builder clearUserId() {
      userId = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'code' field.
      * @return The value.
      */
    public String getCode() {
      return code;
    }


    /**
      * Sets the value of the 'code' field.
      * @param value The value of 'code'.
      * @return This builder.
      */
    public Builder setCode(String value) {
      validate(fields()[4], value);
      this.code = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'code' field has been set.
      * @return True if the 'code' field has been set, false otherwise.
      */
    public boolean hasCode() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'code' field.
      * @return This builder.
      */
    public Builder clearCode() {
      code = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'point' field.
      * @return The value.
      */
    public int getPoint() {
      return point;
    }


    /**
      * Sets the value of the 'point' field.
      * @param value The value of 'point'.
      * @return This builder.
      */
    public Builder setPoint(int value) {
      validate(fields()[5], value);
      this.point = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'point' field has been set.
      * @return True if the 'point' field has been set, false otherwise.
      */
    public boolean hasPoint() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'point' field.
      * @return This builder.
      */
    public Builder clearPoint() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'redeemedAt' field.
      * @return The value.
      */
    public long getRedeemedAt() {
      return redeemedAt;
    }


    /**
      * Sets the value of the 'redeemedAt' field.
      * @param value The value of 'redeemedAt'.
      * @return This builder.
      */
    public Builder setRedeemedAt(long value) {
      validate(fields()[6], value);
      this.redeemedAt = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'redeemedAt' field has been set.
      * @return True if the 'redeemedAt' field has been set, false otherwise.
      */
    public boolean hasRedeemedAt() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'redeemedAt' field.
      * @return This builder.
      */
    public Builder clearRedeemedAt() {
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'expiredAt' field.
      * @return The value.
      */
    public long getExpiredAt() {
      return expiredAt;
    }


    /**
      * Sets the value of the 'expiredAt' field.
      * @param value The value of 'expiredAt'.
      * @return This builder.
      */
    public Builder setExpiredAt(long value) {
      validate(fields()[7], value);
      this.expiredAt = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'expiredAt' field has been set.
      * @return True if the 'expiredAt' field has been set, false otherwise.
      */
    public boolean hasExpiredAt() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'expiredAt' field.
      * @return This builder.
      */
    public Builder clearExpiredAt() {
      fieldSetFlags()[7] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public VoucherRedeemedSchema build() {
      try {
        VoucherRedeemedSchema record = new VoucherRedeemedSchema();
        record.eventId = fieldSetFlags()[0] ? this.eventId : (String) defaultValue(fields()[0]);
        record.timestamp = fieldSetFlags()[1] ? this.timestamp : (Long) defaultValue(fields()[1]);
        record.voucherId = fieldSetFlags()[2] ? this.voucherId : (String) defaultValue(fields()[2]);
        record.userId = fieldSetFlags()[3] ? this.userId : (String) defaultValue(fields()[3]);
        record.code = fieldSetFlags()[4] ? this.code : (String) defaultValue(fields()[4]);
        record.point = fieldSetFlags()[5] ? this.point : (Integer) defaultValue(fields()[5]);
        record.redeemedAt = fieldSetFlags()[6] ? this.redeemedAt : (Long) defaultValue(fields()[6]);
        record.expiredAt = fieldSetFlags()[7] ? this.expiredAt : (Long) defaultValue(fields()[7]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<VoucherRedeemedSchema>
    WRITER$ = (org.apache.avro.io.DatumWriter<VoucherRedeemedSchema>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<VoucherRedeemedSchema>
    READER$ = (org.apache.avro.io.DatumReader<VoucherRedeemedSchema>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.eventId);

    out.writeLong(this.timestamp);

    out.writeString(this.voucherId);

    out.writeString(this.userId);

    out.writeString(this.code);

    out.writeInt(this.point);

    out.writeLong(this.redeemedAt);

    out.writeLong(this.expiredAt);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.eventId = in.readString();

      this.timestamp = in.readLong();

      this.voucherId = in.readString();

      this.userId = in.readString();

      this.code = in.readString();

      this.point = in.readInt();

      this.redeemedAt = in.readLong();

      this.expiredAt = in.readLong();

    } else {
      for (int i = 0; i < 8; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.eventId = in.readString();
          break;

        case 1:
          this.timestamp = in.readLong();
          break;

        case 2:
          this.voucherId = in.readString();
          break;

        case 3:
          this.userId = in.readString();
          break;

        case 4:
          this.code = in.readString();
          break;

        case 5:
          this.point = in.readInt();
          break;

        case 6:
          this.redeemedAt = in.readLong();
          break;

        case 7:
          this.expiredAt = in.readLong();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










