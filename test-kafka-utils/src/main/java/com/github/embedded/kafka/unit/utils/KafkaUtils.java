package com.github.embedded.kafka.unit.utils;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * Utility class for simple <b></>producing/consuming</b> any messages into <b></>Kafka</b>
 */
public class KafkaUtils {
    private final Map<String, Object> configuration;

    /**
     * @param configuration Kafka configuration, <b>should contain bootstrap.servers key</b>
     */
    public KafkaUtils(Map<String, Object> configuration) {
        this.configuration = Collections.unmodifiableMap(configuration);
    }

    /**
     * @param keySerializer     Serializer class for key
     * @param valueSerializer   Serializer class for value
     * @return                  Base configuration needed for kafka's key-value producing
     */
    public <K, V> Map<String, Object> getProducerConfiguration(Map<String, Object> configuration, 
                                                               Class<? extends Serializer<K>> keySerializer,
                                                               Class<? extends Serializer<V>> valueSerializer) {
        Map<String, Object> result = getConfiguration(configuration);
        result.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        result.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return result;
    }

    /**
     * @param valueSerializer   Serializer class for value
     * @return                  Base configuration needed for kafka's value producing
     */
    public <V> Map<String, Object> getProducerConfiguration(Map<String, Object> configuration,
                                                            Class<? extends Serializer<V>> valueSerializer) {
        return getProducerConfiguration(configuration,
                                        ByteArraySerializer.class,
                                        valueSerializer);
    }

    /**
     * @param keySerializer     Serializer class for key
     * @param valueSerializer   Serializer class for value
     * @return                  Base configuration needed for kafka's key-value producing
     */
    public <K, V> Map<String, Object> getProducerConfiguration(Class<? extends Serializer<K>> keySerializer,
                                                               Class<? extends Serializer<V>> valueSerializer) {
        return getProducerConfiguration(null,
                                        keySerializer,
                                        valueSerializer);
    }

    /**
     * @param valueSerializer   Serializer class for value
     * @return                  Base configuration needed for kafka's key-value producing
     */
    public <V> Map<String, Object> getProducerConfiguration(Class<? extends Serializer<V>> valueSerializer) {
        return getProducerConfiguration((Map<String, Object>) null,
                                        valueSerializer);
    }

    /**
     * @param keyDeserializerClass      Deserializer class for key
     * @param valueDeserializerClass    Deserializer class for value
     * @return                          Base configuration needed for kafka's key-value consuming
     */
    public <K, V> Map<String, Object> getConsumerConfiguration(Map<String, Object> configuration,
                                                               Class<? extends Deserializer<K>> keyDeserializerClass,
                                                               Class<? extends Deserializer<V>> valueDeserializerClass) {
        Map<String, Object> result = getConfiguration(configuration);
        result.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        result.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        result.putIfAbsent(GROUP_ID_CONFIG, UUID.randomUUID().toString());
        result.putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return result;
    }

    /**
     * @param valueDeserializerClass    Deserializer class for Value
     * @return                          Base configuration needed for kafka's value consuming
     */
    public <V> Map<String, Object> getConsumerConfiguration(Map<String, Object> configuration,
                                                            Class<? extends Deserializer<V>> valueDeserializerClass) {
        return getConsumerConfiguration(configuration,
                                        ByteArrayDeserializer.class,
                                        valueDeserializerClass);
    }

    /**
     * @param keyDeserializerClass      Deserializer class for key
     * @param valueDeserializerClass    Deserializer class for value
     * @return                          Base configuration needed for kafka's key-value consuming
     */
    public <K, V> Map<String, Object> getConsumerConfiguration(Class<? extends Deserializer<K>> keyDeserializerClass,
                                                               Class<? extends Deserializer<V>> valueDeserializerClass) {
        return getConsumerConfiguration(null,
                                        keyDeserializerClass,
                                        valueDeserializerClass);
    }

    /**
     * @param valueDeserializerClass    Deserializer class for value
     * @return                          Base configuration needed for kafka's key-value consuming
     */
    public <V> Map<String, Object> getConsumerConfiguration(Class<? extends Deserializer<V>> valueDeserializerClass) {
        return getConsumerConfiguration((Map<String, Object>) null,
                                        valueDeserializerClass);
    }

    public <K, V> Stream<Future<RecordMetadata>> produce(Map<String, Object> configuration,
                                                         Class<? extends Serializer<K>> keySerializer,
                                                         Class<? extends Serializer<V>> valueSerializer,
                                                         String topic,
                                                         Stream<Map.Entry<K, V>> data) {
        KafkaProducer<K, V> producer = new KafkaProducer<>(getProducerConfiguration(configuration,
                                                                                    keySerializer,
                                                                                    valueSerializer));
        return data.onClose(producer::close)
                   .map(it -> producer.send(new ProducerRecord<>(topic, it.getKey(), it.getValue())));
    }

    public <V> Stream<Future<RecordMetadata>> produce(Map<String, Object> configuration,
                                                      Class<? extends Serializer<V>> valueSerializer,
                                                      String topic,
                                                      Stream<V> data) {
        return produce(configuration,
                       ByteArraySerializer.class,
                       valueSerializer,
                       topic,
                       data.map(value -> new AbstractMap.SimpleEntry<>(null, value)));
    }

    public <K, V> Stream<Future<RecordMetadata>> produce(Class<? extends Serializer<K>> keySerializer,
                                                         Class<? extends Serializer<V>> valueSerializer,
                                                         String topic,
                                                         Stream<Map.Entry<K, V>> data) {
        return produce(null,
                       keySerializer,
                       valueSerializer,
                       topic,
                       data);
    }

    public <V> Stream<Future<RecordMetadata>> produce(Class<? extends Serializer<V>> valueSerializer,
                                                      String topic,
                                                      Stream<V> data) {
        return produce(null,
                       valueSerializer,
                       topic,
                       data);
    }

    public <K, V> Collection<Future<RecordMetadata>> produce(Map<String, Object> configuration,
                                                             Class<? extends Serializer<K>> keySerializer,
                                                             Class<? extends Serializer<V>> valueSerializer,
                                                             String topic,
                                                             Collection<Map.Entry<K, V>> data) {
        try (Stream<Future<RecordMetadata>> result = produce(configuration,
                                                             keySerializer,
                                                             valueSerializer,
                                                             topic,
                                                             data.stream())) {
            return result.collect(Collectors.toSet());
        }
    }

    public <V> Collection<Future<RecordMetadata>> produce(Map<String, Object> configuration,
                                                          Class<? extends Serializer<V>> valueSerializer,
                                                          String topic,
                                                          Collection<V> data) {
        try (Stream<Future<RecordMetadata>> result = produce(configuration,
                                                             valueSerializer,
                                                             topic,
                                                             data.stream())) {
            return result.collect(Collectors.toSet());
        }
    }

    public <K, V> Collection<Future<RecordMetadata>> produce(Class<? extends Serializer<K>> keySerializer,
                                                             Class<? extends Serializer<V>> valueSerializer,
                                                             String topic,
                                                             Collection<Map.Entry<K, V>> data) {
        return produce(null,
                       keySerializer,
                       valueSerializer,
                       topic,
                       data);
    }

    public <V> Collection<Future<RecordMetadata>> produce(Class<? extends Serializer<V>> valueSerializer,
                                                          String topic,
                                                          Collection<V> data) {
        return produce(null,
                       valueSerializer,
                       topic,
                       data);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Stream<Map.Entry<K, V>> consume(Map<String, Object> configuration,
                                                  Class<? extends Deserializer<K>> keyDeserializerClass,
                                                  Class<? extends Deserializer<V>> valueDeserializerClass,
                                                  Collection<String> topics,
                                                  long pollTimeout,
                                                  int idleCount) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(getConsumerConfiguration(configuration,
                                                                                    keyDeserializerClass,
                                                                                    valueDeserializerClass));
        consumer.subscribe(topics);
        AtomicInteger idleCounter = new AtomicInteger(0);
        return Stream.generate(() -> {
                         ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                         if (!records.isEmpty())
                             idleCounter.set(0);
                         else if (idleCounter.incrementAndGet() > idleCount)
                             throw new RuntimeException("Failed to wait " + idleCounter.get() * pollTimeout
                                                      + " milliseconds for new records from topics " + topics);
                         return records;
                     })
                     .onClose(consumer::close)
                     .flatMap(records -> StreamSupport.stream(records.spliterator(), false))
                     .map(record -> new AbstractMap.SimpleEntry<>(record.key(), record.value()));
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Stream<Map.Entry<K, V>> consume(Map<String, Object> configuration,
                                                  Class<? extends Deserializer<K>> keyDeserializerClass,
                                                  Class<? extends Deserializer<V>> valueDeserializerClass,
                                                  String topic,
                                                  long pollTimeout,
                                                  int idleCount) {
        return consume(configuration,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       Collections.singleton(topic),
                       pollTimeout,
                       idleCount);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Stream<V> consume(Map<String, Object> configuration,
                                 Class<? extends Deserializer<V>> valueDeserializerClass,
                                 Collection<String> topics,
                                 long pollTimeout,
                                 int idleCount) {
        return consume(configuration,
                       ByteArrayDeserializer.class,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount)
                .map(Map.Entry::getValue);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Stream<V> consume(Map<String, Object> configuration,
                                 Class<? extends Deserializer<V>> valueDeserializerClass,
                                 String topic,
                                 long pollTimeout,
                                 int idleCount) {
        return consume(configuration,
                       valueDeserializerClass,
                       Collections.singleton(topic),
                       pollTimeout,
                       idleCount);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Stream<Map.Entry<K, V>> consume(Class<? extends Deserializer<K>> keyDeserializerClass,
                                                  Class<? extends Deserializer<V>> valueDeserializerClass,
                                                  Collection<String> topics,
                                                  long pollTimeout,
                                                  int idleCount) {
        return consume(null,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount);
    }

    public <K, V> Stream<Map.Entry<K, V>> consume(Class<? extends Deserializer<K>> keyDeserializerClass,
                                                  Class<? extends Deserializer<V>> valueDeserializerClass,
                                                  String topic,
                                                  long pollTimeout,
                                                  int idleCount) {
        return consume(null,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       topic,
                       pollTimeout,
                       idleCount);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Stream<V> consume(Class<? extends Deserializer<V>> valueDeserializerClass,
                                 Collection<String> topics,
                                 long pollTimeout,
                                 int idleCount) {
        return consume((Map<String, Object>) null,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Stream<V> consume(Class<? extends Deserializer<V>> valueDeserializerClass,
                                 String topic,
                                 long pollTimeout,
                                 int idleCount) {
        return consume((Map<String, Object>) null,
                       valueDeserializerClass,
                       topic,
                       pollTimeout,
                       idleCount);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Collection<Map.Entry<K, V>> consume(Map<String, Object> configuration,
                                                      Class<? extends Deserializer<K>> keyDeserializerClass,
                                                      Class<? extends Deserializer<V>> valueDeserializerClass,
                                                      Collection<String> topics,
                                                      long pollTimeout,
                                                      int idleCount,
                                                      long size) {
        try (Stream<Map.Entry<K, V>> result = consume(configuration,
                                                      keyDeserializerClass,
                                                      valueDeserializerClass,
                                                      topics,
                                                      pollTimeout,
                                                      idleCount)) {
            return result.limit(size)
                         .collect(Collectors.toList());
        }
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Collection<Map.Entry<K, V>> consume(Map<String, Object> configuration,
                                                      Class<? extends Deserializer<K>> keyDeserializerClass,
                                                      Class<? extends Deserializer<V>> valueDeserializerClass,
                                                      String topic,
                                                      long pollTimeout,
                                                      int idleCount,
                                                      long size) {
        return consume(configuration,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       Collections.singleton(topic),
                       pollTimeout,
                       idleCount,
                       size);

    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Collection<V> consume(Map<String, Object> configuration,
                                     Class<? extends Deserializer<V>> valueDeserializerClass,
                                     Collection<String> topics,
                                     long pollTimeout,
                                     int idleCount,
                                     long size) {
        return consume(configuration,
                       ByteArrayDeserializer.class,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount,
                       size).stream()
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Collection<Map.Entry<K, V>> consume(Class<? extends Deserializer<K>> keyDeserializerClass,
                                                      Class<? extends Deserializer<V>> valueDeserializerClass,
                                                      Collection<String> topics,
                                                      long pollTimeout,
                                                      int idleCount,
                                                      long size) {
        return consume(null,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount,
                       size);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <K, V> Collection<Map.Entry<K, V>> consume(Class<? extends Deserializer<K>> keyDeserializerClass,
                                                      Class<? extends Deserializer<V>> valueDeserializerClass,
                                                      String topic,
                                                      long pollTimeout,
                                                      int idleCount,
                                                      long size) {
        return consume(null,
                       keyDeserializerClass,
                       valueDeserializerClass,
                       topic,
                       pollTimeout,
                       idleCount,
                       size);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Collection<V> consume(Class<? extends Deserializer<V>> valueDeserializerClass,
                                     Collection<String> topics,
                                     long pollTimeout,
                                     int idleCount,
                                     long size) {
        return consume((Map<String, Object>) null,
                       valueDeserializerClass,
                       topics,
                       pollTimeout,
                       idleCount,
                       size);
    }

    /**
     * @param pollTimeout Timeout in millis for one consumer's poll
     * @param idleCount   How many times should consumer try to receive messages after empty poll
     */
    public <V> Collection<V> consume(Class<? extends Deserializer<V>> valueDeserializerClass,
                                     String topic,
                                     long pollTimeout,
                                     int idleCount,
                                     long size) {
        return consume((Map<String, Object>) null,
                       valueDeserializerClass,
                       Collections.singleton(topic),
                       pollTimeout,
                       idleCount,
                       size);
    }

    Map<String, Object> getConfiguration(Map<String, Object> configuration) {
        Map<String, Object> result = Optional.ofNullable(configuration)
                .map(HashMap::new)
                .orElseGet(HashMap::new);
        result.putAll(this.configuration);
        return result;
    }
}
