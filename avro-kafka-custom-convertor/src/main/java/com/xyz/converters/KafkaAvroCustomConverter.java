package com.xyz.converters;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.xyz.converters.AWSKafkaAvroConverterConfig;
import com.xyz.converters.avrodata.AvroData;
import com.xyz.converters.avrodata.AvroDataConfig;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 
 * custom Avro converter for Kafka Connect users.
 */
@Slf4j
@Data
public class KafkaAvroCustomConverter implements Converter {
    private AWSKafkaAvroSerializer serializer;
    private AWSKafkaAvroDeserializer deserializer;
    private AvroData avroData;

    private boolean isKey;
    private final LoadingCache<UUID, GetSchemaVersionResponse> cache;

    /**
     * Constructor used by Kafka Connect user.
     */
    public KafkaAvroCustomConverter() {
        serializer = new AWSKafkaAvroSerializer();
        serializer.setUserAgentApp(UserAgents.KAFKACONNECT);

        deserializer = new AWSKafkaAvroDeserializer();
        deserializer.setUserAgentApp(UserAgents.KAFKACONNECT);
        cache = initializeCache();
    }

    public KafkaAvroCustomConverter(
            AWSKafkaAvroSerializer awsKafkaAvroSerializer,
            AWSKafkaAvroDeserializer awsKafkaAvroDeserializer,
            AvroData avroData) {
        serializer = awsKafkaAvroSerializer;
        deserializer = awsKafkaAvroDeserializer;
        this.avroData = avroData;
        cache = initializeCache();
    }

    /**
     * Configure the AWS Avro Converter.
     * 
     * @param configs configuration elements for the converter
     * @param isKey   true if key, false otherwise
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        new AWSKafkaAvroConverterConfig(configs);

        serializer.configure(configs, this.isKey);
        deserializer.configure(configs, this.isKey);
        avroData = new AvroData(new AvroDataConfig(configs));

    }

    /**
     * Convert orginal Connect data to AVRO serialized byte array
     * 
     * @param topic  topic name
     * @param schema original Connect schema
     * @param value  original Connect data
     * @return AVRO serialized byte array
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            Object avroValue = avroData.fromConnectData(schema, value);
            return serializer.serialize(topic, avroValue);
        } catch (SerializationException | AWSSchemaRegistryException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert AVRO serialized byte array to Connect schema and data
     * 
     * @param topic topic name
     * @param value AVRO serialized byte array
     * @return Connect schema and data
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        Object deserialized;
        GetSchemaVersionResponse getSchemaVersionResponse;
        if (value == null) {
            return SchemaAndValue.NULL;
        }

        try {

            deserialized = deserializer.deserialize(topic, value);
            GlueSchemaRegistryDeserializerDataParser dataParser = GlueSchemaRegistryDeserializerDataParser
                    .getInstance();

            UUID schemaVersionId = dataParser.getSchemaVersionId(ByteBuffer.wrap(value));
            getSchemaVersionResponse = cache.get(schemaVersionId);
        } catch (Exception e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to deserialization error: ", e);
        }
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema avroSchema = parser.parse(getSchemaVersionResponse.schemaDefinition());
        return avroData.toConnectData(avroSchema, deserialized,
                Math.toIntExact(getSchemaVersionResponse.versionNumber()));
    }

    public LoadingCache<UUID, GetSchemaVersionResponse> initializeCache() {
        return CacheBuilder
                .newBuilder()
                .maximumSize(200)
                .refreshAfterWrite(24, TimeUnit.HOURS)
                .build(new GlueSchemaRegistryDeserializationCacheLoader());
    }

    private class GlueSchemaRegistryDeserializationCacheLoader extends CacheLoader<UUID, GetSchemaVersionResponse> {

        public GetSchemaVersionResponse load(UUID schemaVersionId) {
            return deserializer.getGlueSchemaRegistryDeserializationFacade()
                    .getSchemaRegistryClient()
                    .getSchemaVersionResponse(schemaVersionId.toString());
        }
    }
}