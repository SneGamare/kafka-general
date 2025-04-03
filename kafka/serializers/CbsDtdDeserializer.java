package com.kmbl.cros.accountinquiryservice.service.kafka.serializers;

import com.kmbl.cros.accountinquiryservice.event.schema.DtdEvents.DailyTranDetailTable;

/**
 * Avro Deserializer for DailyTranDetailTable.
 *
 * <p>
 * This class extends the {@link AvroDeserializer} to provide deserialization functionality specific
 * to the Avro schema of the {@code DailyTranDetailTable}.
 *
 * <p>
 * Usage: - Create an instance of this class to perform deserialization of Avro-encoded data
 * corresponding to the {@code DailyTranDetailTable} schema.
 *
 * @see AvroDeserializer
 * @see DailyTranDetailTable
 */
public class CbsDtdDeserializer extends AvroDeserializer<DailyTranDetailTable> {

  /**
   * Constructor for creating a CbsDTDDeserializer.
   *
   * <p>
   * Calls the superclass constructor to initialize the deserializer with the Avro schema class for
   * DailyTranDetailTable.
   */
  public CbsDtdDeserializer() {
    super(DailyTranDetailTable.class);
  }
}