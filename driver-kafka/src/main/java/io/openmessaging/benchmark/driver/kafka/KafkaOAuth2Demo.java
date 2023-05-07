/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.kafka;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaOAuth2Demo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties commonProps = new Properties();
        commonProps.put(
                "bootstrap.servers",
                "stress-459-4b36f1a7-6b17-4c3d-83c4-cebe3e872bf1.use4-dog.g.sn3.dev:9093");
        commonProps.put("security.protocol", "SASL_SSL");
        commonProps.put(
                "sasl.login.callback.handler.class",
                "io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler");
        commonProps.put("sasl.mechanism", "OAUTHBEARER");
        commonProps.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                        + "oauth.issuer.url=\"https://auth.sncloud-stg.dev/\" "
                        + "oauth.credentials.url=\"file:///Users/ran/Work/workspaces/fork/benchmark/"
                        + "driver-kafka/o-x388b-stress-admin.json\" "
                        + "oauth.audience=\"urn:sn:pulsar:o-x388b:stress-459\";");
        AdminClient adminClient = AdminClient.create(commonProps);

        String topic = "stress-t2";
        ArrayList<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(topic, 10, (short) 1));
        adminClient.createTopics(topics);

        Properties producerProps = new Properties();
        producerProps.putAll(commonProps);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put("acks", "all");
        producerProps.put("linger.ms", 1);
        producerProps.put("batch.size", 1048576);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, "key", "content-" + i)).get();
            System.out.println("send message " + i);
        }

        Properties consumerProps = new Properties();
        consumerProps.putAll(commonProps);
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", true);
        consumerProps.put("max.partition.fetch.bytes", 10485760);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));

        Thread.sleep(2000);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("receive message " + record.value());
            }
        }

        adminClient.close();
        producer.close();
        consumer.close();
    }
}
