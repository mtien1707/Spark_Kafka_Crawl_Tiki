//package producer;
//
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.Properties;
//
//
//public class Producer {
//    private static final Logger logger = LogManager.getLogger();
//
//    public static void main(String[] args) {
//        String data_path = "E:/FinalProject-Masterdev-Domain_DATA/Final_Project/data/data_tiki_final.csv";
//        Properties props = new Properties();
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "huyvv20-java-producer");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(props);
//
//        String topicName = "huyvv20_tiki";
//        logger.info("Starting load ...");
//
//        try {
//            File file = new File(data_path);
//            FileReader fr = new FileReader(file);
//            BufferedReader br = new BufferedReader(fr);
//            String line = "";
//            while((line = br.readLine()) != null) {
//                kafkaProducer.send(new ProducerRecord<>(topicName,null,line));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//
//        }
//        kafkaProducer.close();
//        logger.info("Finish");
//
//    }
//}
