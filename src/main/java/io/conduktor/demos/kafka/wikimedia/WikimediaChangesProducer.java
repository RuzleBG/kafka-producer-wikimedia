package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws URISyntaxException {
        String bootstrapServers = "localhost:9092";
        String topicName = "wikimedia.recentchanges";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer, topicName);



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
}
