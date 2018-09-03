# Embedded kafka unit
It allows you to start and stop a single node Kafka broker instance for testing applications that use Kafka

## Versions
<table>
    <tr>
        <th align="left">Embedded kafka unit</th>
        <th align="left">Kafka</th>
    </tr>
    <tr>
        <td align="left">0.0.1</td>
        <td align="left">kafka_2.11:0.10.1.2.6.3.0-235</td>
    </tr>
</table>

## Gradle
```groovy
repositories {
	mavenCentral()
	maven { url 'https://jitpack.io' }
	maven { url "http://repo.hortonworks.com/content/repositories/releases/" }
}

dependencies {
	testCompile 'com.github.PakhomovAlexander:embedded-kafka-unit:develop~main-SNAPSHOT'
	testCompile 'org.apache.kafka:kafka-clients:0.10.1.2.6.3.0-235'
	testCompile 'org.apache.kafka:kafka_2.11:0.10.1.2.6.3.0-235:test'
	testCompile 'org.apache.kafka:kafka-clients:0.10.1.2.6.3.0-235:test'
} 
```


## Start using kafka broker
To start a Kafka broker on random port with Zookeeper under the hood use:
```java
TestKafka kafka = new TestKafka();
```

You can choose a port for a Kafka broker:
```java
TestKafka kafka = new TestKafka(6667);
```

Or use JUnitRule:
```java
@Rule
public TestKafkaRule kafkaRule = new TestKafkaRule();
...
TestKafka kafka = kafkaRule.getKafka();
```

Then using KafkaUtils module you can consume and produce to the broker:
```java
public class EmbeddedKafkaTest {
    private static final String TOPIC_NAME = "test-kafka";
    private static final long POLL_TIMEOUT = 100;
    private static final int IDLE_COUNT = 2;

    private List<SampleMessage> messages = ImmutableList.of(
            new SampleMessage(1, "first message"),
            new SampleMessage(2, "second message"),
            new SampleMessage(3, "third message")
    );

    private List<String> jsons = messages.stream()
                                         .map(SampleMessage::toJson)
                                         .collect(Collectors.toList());

    @Rule
    public TestKafkaRule kafkaRule = new TestKafkaRule();

    @Test
    public void messagesProducedAndConsumed() {
        KafkaUtils utils = new KafkaUtils(kafkaRule.getKafka().getConfiguration());

        utils.produce(StringSerializer.class, TOPIC_NAME, jsons);

        Collection<String> consumedMessages = utils.consume(StringDeserializer.class,
                                                            TOPIC_NAME,
                                                            POLL_TIMEOUT, IDLE_COUNT,
                                                            messages.size()
        );

        assertEquals(jsons, consumedMessages);
    }
}
```

If you don't want to use <code>KafkaUtils</code> you can call <code>testKafka.getConfiguration</code> and use it by yourself.

## License
<pre><code>
Copyright 2013 Christopher Batey

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
</code></pre>