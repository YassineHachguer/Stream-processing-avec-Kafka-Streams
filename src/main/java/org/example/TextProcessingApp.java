package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;

public class TextProcessingApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textStream = builder.stream("text-input");


        KStream<String, String>[] branches = textStream.mapValues(value -> {
            if (value == null) return "";
            // 1. trim + espaces multiples → 1 + majuscules
            String cleaned = value.trim().replaceAll("\\s+", " ").toUpperCase();
            return cleaned;
        }).branch(

                (key, value) -> isValid(value),

                (key, value) -> true
        );


        branches[0].to("text-clean");        // valides
        branches[1].to("text-dead-letter"); // invalides

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static boolean isValid(String value) {
        if (value.isEmpty() || value.length() > 100) return false;
        String[] forbidden = {"HACK", "SPAM", "XXX"};
        return Arrays.stream(forbidden).noneMatch(value::contains);
    }
}
