package com.kmbl.cros.accountinquiryservice.service.kafka.serializers;

import com.kmbl.cros.accountinquiryservice.event.schema.GamEvents.GeneralAcctMastTable;

/**
 * Avro Deserializer for GeneralAcctMastTable.
 *
 * <p>
 * This class extends the {@link AvroDeserializer} to provide deserialization functionality specific
 * to the Avro schema of the {@code GeneralAcctMastTable}.
 *
 * <p>
 * Usage: - Create an instance of this class to perform deserialization of Avro-encoded data
 * corresponding to the {@code GeneralAcctMastTable} schema.
 *
 * @see AvroDeserializer
 * @see GeneralAcctMastTable
 */
public class CbsGamDeserializer extends AvroDeserializer<GeneralAcctMastTable> {

  /**
   * Constructor for creating a CbsGAMDeserializer.
   *
   * <p>
   * Calls the superclass constructor to initialize the deserializer with the Avro schema class for
   * GeneralAcctMastTable.
   */
  public CbsGamDeserializer() {
    super(GeneralAcctMastTable.class);
  }
}