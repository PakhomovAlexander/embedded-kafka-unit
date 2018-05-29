package com.github.embedded.kafka.unit;

import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Simple single-node embedded Kafka cluster for integration testing
 */
public class TestKafka implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(TestKafka.class);

    /**
     * Simple test Zookeeper server
     */
    private final TestingServer zookeeper;

    /**
     * Log directory for Kafka Server
     */
    private final File logDir;

    /**
     * Simple test Kafka server {@link TestUtils#createServer}
     */
    private final KafkaServer kafka;

    /**
     * Startup Kafka cluster with default configuration on random port
     */
    public TestKafka() {
        this(null);
    }

    /**
     * Startup Kafka cluster with custom configuration {@link KafkaConfig}
     * Properties {@literal zookeeper.connect} and {@literal log.dir} will be overridden with log warning
     *
     * @param configuration Kafka server configuration
     */
    public TestKafka(Map<String, Object> configuration) {
        try {
            log.info("Starting up Zookeeper server");
            zookeeper = new TestingServer();
            log.info("Started up Zookeeper server on port {}", zookeeper.getPort());

            logDir = Files.createTempDir();
            log.info("Created log directory for Kafka server {}", logDir);

            log.info("Starting up Kafka server");
            kafka = TestUtils.createServer(new KafkaConfig(prepareConfiguration(configuration), true), SystemTime$.MODULE$);
            kafka.startup();
            log.info("Started up Kafka server on port {}", getPort());
        } catch (Exception exception) {
            close();
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void close() {
        Optional.ofNullable(kafka).ifPresent(it -> {
            try {
                log.info("Shutdown Kafka server on port {}", getPort());
                it.shutdown();
                it.awaitShutdown();
            } catch (Exception exception) {
                log.error("Failed to shutdown Kafka server on port {}", getPort(), exception);
            }
        });

        Optional.ofNullable(zookeeper).ifPresent(it -> {
            log.info("Shutdown Zookeeper server on port {}", it.getPort());
            try {
                it.close();
            } catch (Exception exception) {
                log.error("Failed to shutdown Zookeeper server on port {}", it.getPort(), exception);
            }
        });

        Optional.ofNullable(logDir).ifPresent(it -> {
            try {
                log.info("Delete Kafka log directory {}", it);
                FileUtils.deleteDirectory(it);
            } catch (Exception exception) {
                log.error("Filed to delete Kafka log directory {}", it, exception);
            }
        });
    }

    /**
     * @return Kafka server port
     */
    public int getPort() {
        return kafka.boundPort(SecurityProtocol.PLAINTEXT);
    }

    /**
     * @return Kafka configuration {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG} property value
     */
    public String getBootstrapServers() {
        return "localhost:" + getPort();
    }

    /**
     * @return Minimal base configuration for Kafka's {@link KafkaConsumer} and {@link KafkaProducer}
     */
    public Map<String, Object> getConfiguration() {
        return getConfiguration(new HashMap<>());
    }

    /**
     * @return Minimal base configuration for Kafka's {@link KafkaConsumer} and {@link KafkaProducer}
     */
    public Map<String, Object> getConfiguration(Map<String, Object> configuration) {
        configuration.put(BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        return configuration;
    }

    Map<String, Object> prepareConfiguration(Map<String, Object> customConfiguration) throws IOException {
        Map<String, Object> configuration = new HashMap<>(Optional.ofNullable(customConfiguration).orElse(Collections.emptyMap()));

        if (configuration.containsKey(KafkaConfig$.MODULE$.PortProp()))
            log.warn("Using same port {} for Kafka server in multi-threading tests can lead to errors", configuration.get(KafkaConfig$.MODULE$.PortProp()));
        else
            overrideConfigurationProperty(configuration, KafkaConfig$.MODULE$.PortProp(), getFreePort());

        overrideConfigurationProperty(configuration, KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.getConnectString());
        overrideConfigurationProperty(configuration, KafkaConfig$.MODULE$.LogDirProp(), logDir.getAbsolutePath());

        return configuration;
    }

    static void overrideConfigurationProperty(Map<String, Object> configuration,
                                              String property,
                                              Object defaultValue) {
        configuration.compute(property, (key, value) -> {
            if (value != null)
                log.warn("Custom property {} value {} is overridden by default value {}", key, value, defaultValue);
            return defaultValue;
        });
    }

    static int getFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
