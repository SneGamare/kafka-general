package com.kmbl.cros.accountinquiryservice.service.kafka.serializers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;


public class CbsAvroSerializer<T extends org.apache.avro.specific.SpecificRecordBase> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {

        try {
            DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            outputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Serialization failed for: " + data, e);
        }

    }

}