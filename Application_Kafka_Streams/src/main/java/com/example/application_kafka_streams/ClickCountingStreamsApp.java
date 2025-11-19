package com.example.application_kafka_streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;

@Component
public class ClickCountingStreamsApp {

    private KafkaStreams streams;

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> clicksStream = builder.stream("clicks", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> clickCounts = clicksStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("click-counts-store"));

        clickCounts.toStream().to("click-counts", Produced.with(Serdes.String(), Serdes.Long()));

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    @PreDestroy
    public void close() {
        if (streams != null) streams.close();
    }
}
