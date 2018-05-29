package com.github.embedded.kafka.unit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import kafka.server.KafkaConfig$;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestKafkaTest {
    static final String TOPIC = "test-topic";

    static final Collection<Map.Entry<Integer, String>> DATA = ImmutableList.of(
            Maps.immutableEntry(0, "value 0"),
            Maps.immutableEntry(1, "value 1"),
            Maps.immutableEntry(2, "value 2"),
            Maps.immutableEntry(3, "value 3"),
            Maps.immutableEntry(0, "value 0")
    );

    static final long POLL_TIMEOUT = 100;

    /**
     * The number of empty poll's attempts
     */
    static final int IDLE_COUNT = 3;

    @Test
    @Disabled
    void close() {
        //TODO
    }

    @Test
    void defaultConfiguration() {
        try (TestKafka kafka = new TestKafka()) {
            int port = kafka.getPort();
            String bootstrapServers = "localhost:" + port;

            assertAll(() -> assertEquals(bootstrapServers,
                                         kafka.getBootstrapServers(),
                                        "Wrong bootstrap servers in default configuration"),
                      () -> assertEquals(ImmutableMap.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                                         kafka.getConfiguration(),
                                        "Wrong configuration"));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ConfigurationMapProvider.class)
    void customConfiguration(Map<String, Object> configuration) {
        if (configuration == null)
            return;

        Object port = configuration.get(KafkaConfig$.MODULE$.PortProp());

        try (TestKafka kafka = new TestKafka(configuration)) {
            String bootstrapServers = "localhost:" + port;
            assertAll(() -> assertEquals(bootstrapServers,
                                         kafka.getBootstrapServers(),
                                         "Wrong bootstrap servers with custom configuration"),
                      () -> assertEquals(ImmutableMap.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                                         kafka.getConfiguration(),
                                        "Wrong configuration"),
                      () -> assertEquals(port,
                                         kafka.getPort(),
                                        "Actual kafka port and port from init configuration are different"));
        }
    }

    @Test
    void emptyConstructor() {
        try (TestKafka kafka = new TestKafka()) {
            assertAll(() -> assertDoesNotThrow(() -> produce(kafka),
                                               "Producer fail with empty constructor"),

                      () -> assertEquals(DATA,
                                         consume(kafka),
                                         "Wrong consumed data from TestKafka initialized by empty constructor"));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ConfigurationMapProvider.class)
    void nonEmptyConstructor(ImmutableMap<String, Object> configuration) {
        try (TestKafka kafka = new TestKafka(configuration)) {
            assertAll(() -> assertDoesNotThrow(() -> produce(kafka),
                                               "Producer fail with empty constructor"),
                      () -> assertEquals(DATA,
                                         consume(kafka),
                                        "Wrong consumed data from TestKafka initialized by empty constructor"));
        }
    }

    void produce(TestKafka kafka) {
        try (Producer<Integer, String> producer = new KafkaProducer<>(configurationBuilder(kafka)
                                                                              .put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                                                                              .put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                                                                              .build())) {
            DATA.forEach(entry -> producer.send(new ProducerRecord<Integer, String>(TOPIC, entry.getKey(), entry.getValue())));
        }
    }

    private Collection<Map.Entry<Integer, String>> consume(TestKafka kafka) {
        try (KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(configurationBuilder(kafka)
                                                                                   .put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                                                                                   .put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                                                                   .put(GROUP_ID_CONFIG, "test-group-id")
                                                                                   .put(AUTO_OFFSET_RESET_CONFIG, "earliest")
                                                                                   .build())) {
            AtomicInteger idleCounter = new AtomicInteger(0);
            consumer.subscribe(Collections.singletonList(TOPIC));
            return Stream.generate(() -> {
                             ConsumerRecords<Integer, String> records = consumer.poll(POLL_TIMEOUT);
                             if (!records.isEmpty())
                                 idleCounter.set(0);
                             else if (idleCounter.incrementAndGet() > IDLE_COUNT)
                                 throw new RuntimeException("Failed to wait " + idleCounter.get() * POLL_TIMEOUT
                                                                    + " milliseconds for new records from topic " + TOPIC);
                             return records;
                         })
                         .flatMap(records -> StreamSupport.stream(records.spliterator(), false))
                         .limit(DATA.size())
                         .map(record -> Maps.immutableEntry(record.key(), record.value()))
                         .collect(ImmutableList.toImmutableList());
        }
    }

    ImmutableMap.Builder<String, Object> configurationBuilder(TestKafka kafka) {
        return ImmutableMap.<String, Object>builder()
                .putAll(kafka.getConfiguration());
    }

    static class ConfigurationMapProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            return Stream.of(null,
                             ImmutableMap.of(KafkaConfig$.MODULE$.PortProp(), TestKafka.getFreePort(),
                                             KafkaConfig$.MODULE$.ZkConnectProp(), "incorrect connection"))
                         .map(Arguments::of);
        }
    }
}
