package org.schm1tz1.KafkaStreamsTimestampExamples;

import com.github.schm1tz1.kafka.serde.JsonSimpleSerde;
import com.github.schm1tz1.kafka.serde.JsonSimpleSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Properties;

public class kafkaStreamsTransformTimestampDemo {

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkaStreamsTransformTimestampDemo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSimpleSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JSONObject> inStream = builder.stream("inputData");

        inStream
                .transform(AddReceiveTimestampTransformer::new)
                .to("outputData");

        inStream
                .transform(AddReceiveTimestampAndModifyTransformer::new)
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
        System.out.println("kafkaStreamsTransformTimestampDemo started!");
    }

    private static class AddReceiveTimestampAndModifyTransformer extends AddReceiveTimestampTransformer {
        @Override
        void forwardWrapper(String key, JSONObject value) {
            long newTimestamp = Long.parseLong(key);
            System.out.println("changing timestamp to "+newTimestamp+" and forwarding -> "+this.getClass().getName()+", "+value+"\n\n");
            context.forward(this.getClass().getName(), value, To.all().withTimestamp(newTimestamp));
        }
    }

    private static class AddReceiveTimestampTransformer implements Transformer<String, JSONObject, KeyValue<String, JSONObject>> {
        protected ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, JSONObject> transform(String _unusedKey_, JSONObject value) {
            System.out.println(String.format("received: %d (%s/%d/%d): %s", context.timestamp(), context.topic(), context.partition(), context.offset(), value));

            Long timeNowInMillis = System.currentTimeMillis()+1; // add one millisecond to have a difference to context (just in case)

            Object metadataValue = value.get("metadata");
            JSONArray metadataArray = metadataValue instanceof JSONArray ? (JSONArray) metadataValue : new JSONArray();
            JSONObject newMetadata = new JSONObject();
            newMetadata.put("record timestamp received", context.timestamp());
            newMetadata.put("transformed", timeNowInMillis);
            metadataArray.add(newMetadata);

            forwardWrapper(timeNowInMillis.toString(), value);
            return null;
        }

        void forwardWrapper(String key, JSONObject value) {
            System.out.println("forwarding -> "+this.getClass().getName()+", "+value+"\n\n");
            context.forward(this.getClass().getName(), value);
        }

        @Override
        public void close() {

        }
    }
}