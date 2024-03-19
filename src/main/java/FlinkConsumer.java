package consumer;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck;
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy;
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck;

import com.stefan_grafberger.streamdq.anomalydetection.detectors.aggregatedetector.AggregateAnomalyCheck;
import com.stefan_grafberger.streamdq.anomalydetection.strategies.DetectionStrategy;
import com.stefan_grafberger.streamdq.checks.aggregate.AggregateCheck;
import com.stefan_grafberger.streamdq.checks.row.RowLevelCheck;
import com.stefan_grafberger.streamdq.VerificationSuite;

import java.math.BigDecimal;

public class FlinkConsumer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), props);

        env.addSource(kafkaConsumer)
            .process(new StreamDQValidator());

        env.execute("Flink Consumer with StreamDQ Validation");
    }

    public static class StreamDQValidator extends ProcessFunction<String, String> {
        @Override
        public void processElement(String jsonString, Context ctx, Collector<String> out) throws Exception {
            // 行级检查：确保乘客计数在0到5之间

            //VerificationSuite verificationResult = new VerificationSuite();

            //RowLevelCheck rowLevelCheck = new RowLevelCheck()
            //.isInRange("passenger_count", BigDecimal.valueOf(0.0), BigDecimal.valueOf(5.0));

            // 将数据原样输出
            System.out.println(jsonString);
            out.collect(jsonString);
        }
    }
}
