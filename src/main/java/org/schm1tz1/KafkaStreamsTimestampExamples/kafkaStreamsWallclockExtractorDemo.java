package org.schm1tz1.KafkaStreamsTimestampExamples;

import com.github.schm1tz1.kafka.serde.JsonSimpleSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.codehaus.jackson.map.util.ISO8601Utils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Properties;

public class kafkaStreamsWallclockExtractorDemo {

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkaStreamsWallclockExtractorDemo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSimpleSerde.class.getName());
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // one can change the default extractor a below
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();

        // or just use Consumed.with(<TimestampExtractor>)
        KStream<String, JSONObject> inStreamDefault = builder.stream("inputData");
        KStream<String, JSONObject> inStreamWallclock = builder.stream("inputData", Consumed.with(new WallclockTimestampExtractor()));

        inStreamDefault
                .map((k,v)->KeyValue.pair(k,v.put("timestampExtractor", "DEFAULT")))
                .to("outputData");

        inStreamWallclock
                .map((k,v)->KeyValue.pair(k,v.put("timestampExtractor", "WALLCLOCK")))
                .to("outputData");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });

        try {
            streams.start();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.out.println("kafkaStreamsWallclockExtractorDemo started!");
    }
}