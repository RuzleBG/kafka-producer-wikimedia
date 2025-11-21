package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws URISyntaxException {
        BackgroundEventHandler backgroundEventHandler = getBackgroundEventHandler();

        URI uri = new URI("https://stream.wikimedia.org/v2/stream/recentchange");

        ConnectStrategy strategy = ConnectStrategy.http(uri)
                .header("User-Agent",
                        "MyKafkaProducer/1.0 (https://conduktor.io; kirillazarov0@gmail.com)");

        BackgroundEventSource source = new BackgroundEventSource.Builder(backgroundEventHandler, new EventSource.Builder(strategy)).build();
        source.start();

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    @NotNull
    private static BackgroundEventHandler getBackgroundEventHandler() {
        String bootstrapServers = "localhost:9092";
        String topicName = "wikimedia.recentchanges";

        //set safe producer config (only for kafka < 3.0)
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //set safe producer config (only for kafka < 3.0)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        //high throughput configuration
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return new WikimediaChangeHandler(producer, topicName);
    }
}
