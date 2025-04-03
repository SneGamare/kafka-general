package com.kmbl.cros.accountinquiryservice.service.kafka.serializers;

import java.io.ByteArrayInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Avro Deserializer for deserializing Avro-encoded data into a specific type {@code T}.
 *
 * <p>
 * This class implements the {@link Deserializer} interface and provides functionality to
 * deserialize Avro-encoded byte[] data into instances of a specific Avro schema class.
 *
 * @param <T> The generic type representing the Avro schema class.
 *
 * @see Deserializer
 * @see SpecificDatumReader
 * @see BinaryDecoder
 */
@Slf4j
public class AvroDeserializer<T> implements Deserializer<T> {

  protected final Class<T> clazz;

  /**
   * Constructor for creating an Avro Deserializer.
   *
   * @param clazz The class representing the Avro schema for deserialization.
   */
  public AvroDeserializer(Class<T> clazz) {
    this.clazz = clazz;
  }

  /**
   * Deserializes Avro-encoded byte[] data into an instance of the specified Avro schema class.
   *
   * @param topic The topic associated with the data (unused in this implementation).
   * @param data  The Avro-encoded byte[] data to be deserialized.
   * @return An instance of the specified Avro schema class.
   * @throws SerializationException If an error occurs during deserialization.
   */
  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
      T deserlizedData = datumReader.read(null, decoder);
      return deserlizedData;
    } catch (Exception e) {
      log.warn("Failed to deserialize event: {}", e.getMessage());
      throw new SerializationException("Error when deserializing byte[] to " + clazz.getName(), e);
    }
  }

  /**
   * @param topic   The topic associated with the data (unused in this implementation).
   * @param headers The headers associated with the Kafka record (unused in this implementation).
   * @param data    The Avro-encoded byte[] data to be deserialized.
   * @return An instance of the specified Avro schema class.
   */
  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {

    // TODO : Will remove in future ( just for UAT testing )
    String eventString = new String(Hex.encodeHex(data));
    log.debug("CROSAvroDeserializer:deserialize received encodeHex {}", eventString);
    try {
      return deserialize(topic, data);
    } catch (SerializationException e) {
      log.warn("Failed to deserialize event: {}", e.getMessage());
      return null;
    }
  }
}