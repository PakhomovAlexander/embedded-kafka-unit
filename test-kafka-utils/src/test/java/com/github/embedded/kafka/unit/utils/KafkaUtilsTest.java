package com.github.embedded.kafka.unit.utils;

import com.github.embedded.kafka.unit.TestKafka;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaUtilsTest {
    static final String TOPIC = "test-topic";

    TestKafka kafka;

    KafkaUtils utils;

    @BeforeEach
    void setupKafka() {
        kafka = new TestKafka();
        utils = new KafkaUtils(kafka.getConfiguration());
    }

    @AfterEach
    void shutdownKafka() {
        kafka.close();
    }

    @Test
    void getProducerConfiguration() {
        Map<String, Object> configuration = utils.getProducerConfiguration(kafka.getConfiguration(),
                                                                           IntegerSerializer.class,
                                                                           StringSerializer.class);

        assertAll(() -> assertEquals(3, configuration.size()),
                  () -> assertEquals(kafka.getBootstrapServers(), configuration.get(BOOTSTRAP_SERVERS_CONFIG)),
                  () -> assertEquals(IntegerSerializer.class, configuration.get(KEY_SERIALIZER_CLASS_CONFIG)),
                  () -> assertEquals(StringSerializer.class, configuration.get(VALUE_SERIALIZER_CLASS_CONFIG)));
    }

    @Test
    @Disabled
    void getConsumerConfiguration() {
        Map<String, Object> configuration = utils.getConsumerConfiguration(kafka.getConfiguration(),
                                                                           IntegerDeserializer.class,
                                                                           StringDeserializer.class);

        assertAll(() -> assertEquals(5, configuration.size()),
                  () -> assertEquals(kafka.getBootstrapServers(), configuration.get(BOOTSTRAP_SERVERS_CONFIG)),
                  () -> assertEquals(IntegerDeserializer.class, configuration.get(KEY_DESERIALIZER_CLASS_CONFIG)),
                  () -> assertEquals(StringDeserializer.class, configuration.get(VALUE_DESERIALIZER_CLASS_CONFIG)),
                  () -> assertEquals("earliest", configuration.get(AUTO_OFFSET_RESET_CONFIG).toString()));
    }

    @Test
    void keyValueCollections() {
        final Collection<Map.Entry<Integer, String>> DATA = ImmutableList.of(
                Maps.immutableEntry(0, "value 0"),
                Maps.immutableEntry(1, "value 1"),
                Maps.immutableEntry(2, "value 2"),
                Maps.immutableEntry(3, "value 3"),
                Maps.immutableEntry(0, "value 0")
        );

        Collection<Future<RecordMetadata>> metadata = utils.produce(
                IntegerSerializer.class,
                StringSerializer.class,
                TOPIC,
                DATA
        );

        assertAll(() -> assertEquals(DATA.size(),
                                     metadata.size(),
                                    "Metadata size being produced doesn't match actual data size"),
                  () -> assertEquals(DATA,
                                     utils.consume(IntegerDeserializer.class,
                                                   StringDeserializer.class,
                                                   TOPIC,
                                                   100L,
                                                   3,
                                                    DATA.size()),
                                     "Produced data doesn't match consumed data"));
    }


    @Test
    void valueCollections() {
        final Collection<Integer> DATA = ImmutableList.of(0, 1, 2, 4, 4, 5);

        Collection<Future<RecordMetadata>> metadata = utils.produce(IntegerSerializer.class,
                                                                    TOPIC,
                                                                    DATA);

        assertAll(() -> assertEquals(DATA.size(),
                                     metadata.size(),
                                     "Metadata size being produced doesn't match actual data size"),
                  () -> assertEquals(DATA,
                                     utils.consume(IntegerDeserializer.class,
                                                   TOPIC,
                                                   100L,
                                                   3,
                                                    DATA.size()),
                                    "Produced data doesn't match consumed data"));
    }

    @Test
    @Disabled
    void streams() {
        //TODO
    }
}