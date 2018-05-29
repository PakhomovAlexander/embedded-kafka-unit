package com.github.embedded.kafka.unit.rule;

import com.github.embedded.kafka.unit.TestKafka;
import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;

public class TestKafkaRuleTest {
    @Rule
    public TestKafkaRule defaultKafka = new TestKafkaRule();

    @Rule
    public TestKafkaRule existedKafka = new TestKafkaRule(new TestKafka());

    @Rule
    public TestKafkaRule configuredKafka = new TestKafkaRule(ImmutableMap.of());

    @Test
    public void main() {}
}
