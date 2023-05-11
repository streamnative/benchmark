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
package io.openmessaging.benchmark.driver.pulsar;


import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;

public class PulsarBenchmarkTxnProducer implements BenchmarkProducer {

    private final Producer<byte[]> producer;

    private final PulsarClient client;

    public PulsarBenchmarkTxnProducer(Producer<byte[]> producer, PulsarClient client) {
        this.producer = producer;
        this.client = client;
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return buildTransaction()
                .thenCompose(
                        txn -> {
                            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage(txn).value(payload);
                            key.ifPresent(msgBuilder::key);
                            return msgBuilder.sendAsync().thenApply(msgId -> txn);
                        })
                .thenCompose(Transaction::abort);
    }

    private CompletableFuture<Transaction> buildTransaction() {
        try {
            return client.newTransaction().withTransactionTimeout(5, TimeUnit.HOURS).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
