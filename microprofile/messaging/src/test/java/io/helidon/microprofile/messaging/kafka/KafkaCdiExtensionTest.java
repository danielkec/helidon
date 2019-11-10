/*
 * Copyright (c)  2019 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.helidon.microprofile.messaging.kafka;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import io.helidon.config.Config;
import io.helidon.config.ConfigSources;
import io.helidon.messaging.kafka.SimpleKafkaProducer;
import io.helidon.messaging.kafka.connector.KafkaConnectorFactory;
import io.helidon.microprofile.messaging.AbstractCDITest;
import io.helidon.microprofile.messaging.kafka.KafkaConsumingBean;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.helidon.common.CollectionsHelper.mapOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaCdiExtensionTest extends AbstractCDITest {


    @RegisterExtension
    public static final SharedKafkaTestResource kafkaResource = new SharedKafkaTestResource();
    public static final String TEST_TOPIC = "graph-done";
    public static final String TEST_MESSAGE = "this is first test message";

    @Override
    protected Map<String, String> cdiConfig() {
        Map<String, String> p = new HashMap<>();
        p.putAll(mapOf(
                "mp.messaging.incoming.test-channel-1.connector", KafkaConnectorFactory.CONNECTOR_NAME,
                "mp.messaging.incoming.test-channel-1.bootstrap.servers", kafkaResource.getKafkaConnectString(),
                "mp.messaging.incoming.test-channel-1.topic", TEST_TOPIC,
                "mp.messaging.incoming.test-channel-1.key.deserializer", LongDeserializer.class.getName(),
                "mp.messaging.incoming.test-channel-1.value.deserializer", StringDeserializer.class.getName()));
        p.putAll(mapOf(
                "mp.messaging.incoming.test-channel-2.connector", KafkaConnectorFactory.CONNECTOR_NAME,
                "mp.messaging.incoming.test-channel-2.bootstrap.servers", kafkaResource.getKafkaConnectString(),
                "mp.messaging.incoming.test-channel-2.topic", TEST_TOPIC,
                "mp.messaging.incoming.test-channel-2.key.deserializer", LongDeserializer.class.getName(),
                "mp.messaging.incoming.test-channel-2.value.deserializer", StringDeserializer.class.getName())
        );
        return p;
    }

    @Override
    protected void cdiBeanClasses(Set<Class<?>> classes) {
        classes.add(KafkaConnectorFactory.class);
        classes.add(KafkaConsumingBean.class);
    }

    @BeforeAll
    public static void prepareTopics() {
        kafkaResource.getKafkaTestUtils().createTopic(TEST_TOPIC, 10, (short) 1);
    }

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
        //Wait till consumers are ready
        forEachBean(KafkaConnectorFactory.class, KAFKA_CONNECTOR_LITERAL, b -> b.getConsumers().forEach(c -> {
            try {
                c.waitForPartitionAssigment(10, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
                fail(e);
            }
        }));
    }

    @Test
    void incomingKafkaTest() throws InterruptedException {
        // Producer
        Map<String, String> p = mapOf(
                "mp.messaging.outcoming.test-channel.bootstrap.servers", kafkaResource.getKafkaConnectString(),
                "mp.messaging.outcoming.test-channel.topic", TEST_TOPIC,
                "mp.messaging.outcoming.test-channel.key.serializer", LongSerializer.class.getName(),
                "mp.messaging.outcoming.test-channel.value.serializer", StringSerializer.class.getName()
        );

        Config config = Config.builder()
                .sources(ConfigSources.create(p))
                .build();

        SimpleKafkaProducer<Long, String> producer = new SimpleKafkaProducer<>(config.get("mp.messaging.outcoming.test-channel"));
        List<Future<RecordMetadata>> producerFutures = new ArrayList<>(KafkaConsumingBean.TEST_DATA.size());

        //Send all test messages(async send means order is not guaranteed)
        KafkaConsumingBean.TEST_DATA.forEach(msg -> producerFutures.addAll(producer.produceAsync(msg)));

        // Wait for all sent(this is example usage, sent doesn't mean delivered)
        producerFutures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                fail(e);
            }
        });

        // Wait till 3 records are delivered
        assertTrue(KafkaConsumingBean.testChannelLatch.await(15, TimeUnit.SECONDS)
                , "All messages not delivered in time, number of unreceived messages: "
                        + KafkaConsumingBean.testChannelLatch.getCount());
        producer.close();
    }
}