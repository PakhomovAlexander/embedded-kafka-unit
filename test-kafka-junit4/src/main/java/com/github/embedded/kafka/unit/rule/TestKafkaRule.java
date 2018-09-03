package com.github.embedded.kafka.unit.rule;

import com.github.embedded.kafka.unit.TestKafka;
import org.junit.rules.ExternalResource;

import java.util.Map;

public class TestKafkaRule extends ExternalResource {
    private final TestKafka testKafka;

    public TestKafkaRule(TestKafka testKafka) {
        this.testKafka = testKafka;
    }

    public TestKafkaRule() {
        this(new TestKafka());
    }

    public TestKafkaRule(Map<String, Object> configuration) {
        this(new TestKafka(configuration));
    }

    @Override
    protected void after() {
        testKafka.close();
    }

    public TestKafka getKafka() {
        return testKafka;
    }
}
