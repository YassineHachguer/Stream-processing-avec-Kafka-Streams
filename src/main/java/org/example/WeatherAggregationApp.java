package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;


public class WeatherAggregationApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-aggregation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> weatherStream = builder.stream("weather-data", Consumed.with(Serdes.String(), Serdes.String()));


        KStream<String, WeatherValue> parsedAndFiltered = weatherStream
                .mapValues(WeatherAggregationApp::parseCsvToWeatherValue)
                .filter((k, v) -> v != null)
                .filter((k, v) -> v.getTemperatureC() > 30.0)
                .map((k, v) -> {
                    double tempF = celsiusToFahrenheit(v.getTemperatureC());
                    WeatherValue converted = new WeatherValue(v.getStation(), tempF, v.getHumidity());
                    return KeyValue.pair(converted.getStation(), converted); // key = station
                });


        Serde<StationStats> statsSerde = new GsonSerde<>(StationStats.class);

        KGroupedStream<String, WeatherValue> grouped = parsedAndFiltered.groupByKey(Grouped.with(Serdes.String(), new WeatherValueSerde()));

        KTable<String, StationStats> aggregated = grouped.aggregate(
                () -> new StationStats(0.0, 0.0, 0L),
                (station, weatherValue, agg) -> {
                    agg.sumTempF += weatherValue.getTemperatureC(); // here temperatureC actually holds °F after conversion step
                    agg.sumHumidity += weatherValue.getHumidity();
                    agg.count += 1;
                    return agg;
                },
                Materialized.with(Serdes.String(), statsSerde)
        );


        KStream<String, String> results = aggregated.toStream()
                .mapValues((stationStats) -> {
                    double avgTempF = stationStats.count == 0 ? 0.0 : stationStats.sumTempF / stationStats.count;
                    double avgHumidity = stationStats.count == 0 ? 0.0 : stationStats.sumHumidity / stationStats.count;
                    // Format simple : avg temp rounded to 2 decimals and avg humidity rounded to 2 decimals
                    return String.format("%.2f,%.2f", avgTempF, avgHumidity);
                });


        results.to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }


    private static WeatherValue parseCsvToWeatherValue(String line) {
        if (line == null) return null;
        String[] parts = line.split(",", -1);
        if (parts.length < 3) return null;
        try {
            String station = parts[0].trim();
            double temp = Double.parseDouble(parts[1].trim());
            double humidity = Double.parseDouble(parts[2].trim());
            return new WeatherValue(station, temp, humidity);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static double celsiusToFahrenheit(double c) {
        return (c * 9.0 / 5.0) + 32.0;
    }


    static class WeatherValue {
        private String station;
        private double temperatureC; // note: after conversion step this will hold °F for simplicity
        private double humidity;

        public WeatherValue() {}

        public WeatherValue(String station, double temperatureC, double humidity) {
            this.station = station;
            this.temperatureC = temperatureC;
            this.humidity = humidity;
        }

        public String getStation() { return station; }
        public double getTemperatureC() { return temperatureC; }
        public double getHumidity() { return humidity; }

        public void setStation(String station) { this.station = station; }
        public void setTemperatureC(double temperatureC) { this.temperatureC = temperatureC; }
        public void setHumidity(double humidity) { this.humidity = humidity; }
    }


    static class StationStats {
        double sumTempF;
        double sumHumidity;
        long count;

        public StationStats() {}

        public StationStats(double sumTempF, double sumHumidity, long count) {
            this.sumTempF = sumTempF;
            this.sumHumidity = sumHumidity;
            this.count = count;
        }
    }


    static class GsonSerde<T> implements Serde<T> {
        private final Gson gson = new Gson();
        private final Class<T> cls;

        public GsonSerde(Class<T> cls) { this.cls = cls; }

        @Override
        public Serializer<T> serializer() {
            return new Serializer<T>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {}
                @Override
                public byte[] serialize(String topic, T data) {
                    if (data == null) return null;
                    return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
                }
                @Override
                public void close() {}
            };
        }

        @Override
        public Deserializer<T> deserializer() {
            return new Deserializer<T>() {
                @Override
                public void configure(Map<String, ?> configs, boolean isKey) {}
                @Override
                public T deserialize(String topic, byte[] data) {
                    if (data == null) return null;
                    try {
                        String s = new String(data, StandardCharsets.UTF_8);
                        return gson.fromJson(s, cls);
                    } catch (JsonSyntaxException e) {
                        return null;
                    }
                }
                @Override
                public void close() {}
            };
        }
    }


    static class WeatherValueSerde implements Serde<WeatherValue> {
        private final GsonSerde<WeatherValue> inner = new GsonSerde<>(WeatherValue.class);
        @Override public Serializer<WeatherValue> serializer() { return inner.serializer(); }
        @Override public Deserializer<WeatherValue> deserializer() { return inner.deserializer(); }
        @Override public void configure(Map<String, ?> configs, boolean isKey) {}
        @Override public void close() {}
    }
}
