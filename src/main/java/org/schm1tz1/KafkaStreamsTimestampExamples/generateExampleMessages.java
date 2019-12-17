package org.schm1tz1.KafkaStreamsTimestampExamples;

import com.github.schm1tz1.kafka.serde.JsonSimpleSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Properties;
import java.util.Random;

public class generateExampleMessages {
    final private static String PRODUCER_TOPIC = "inputData";
    final private static Random random = new Random();

    private static KafkaProducer createProducer() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "generateExampleMessages");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSimpleSerializer.class.getName());
        return new KafkaProducer<>(producerConfig);
    }

    private static JSONObject makePayload(int messageId) {

        Long now = System.currentTimeMillis();
        JSONObject jsonObject = new JSONObject();

        JSONArray metadataArray = new JSONArray();
        JSONObject firstLine = new JSONObject();
        firstLine.put("created", System.currentTimeMillis());
        metadataArray.add(firstLine);

        jsonObject.put("ID", String.format("%04d", messageId));
        jsonObject.put("metadata", metadataArray);

        return jsonObject;
    }


    public static void main(final String[] args) {

        KafkaProducer kafkaProducer = createProducer();

        int numberOfRecords = Integer.parseInt(System.getProperty("noRecords", "3"));

        System.out.println("Producing " + numberOfRecords + " random records...");
        for (int i = 0; i < numberOfRecords; ++i) {
            JSONObject jsonPayload = makePayload(i);
            sendJsonObject(kafkaProducer, jsonPayload);
        }

        kafkaProducer.close();
    }

    private static void sendJsonObject(KafkaProducer kafkaProducer, JSONObject jsonPayload) {
        ProducerRecord producerRecord = new ProducerRecord<>(PRODUCER_TOPIC, jsonPayload);

        System.out.println(producerRecord.toString());
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
    }

}


