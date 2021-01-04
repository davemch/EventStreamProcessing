package kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import types.Appeal;
import types.Event;
import types.Refuse;

import java.util.Properties;

public class Kafka {
    private final String TOPIC;
    private final Properties properties = new Properties();

    // TODO: Maye move to Event-Types?
    // Producers
    public FlinkKafkaProducer<Event> simpleEventProducer;
    public FlinkKafkaProducer<Appeal> appealProducer;
    public FlinkKafkaProducer<Refuse> refuseProducer;

    public Kafka(String topic, String serverName, String serverLocation) {
        this.TOPIC = topic;
        this.properties.setProperty(serverName, serverLocation);
    }

    public Kafka initProducers() {
        this.simpleEventProducer = new FlinkKafkaProducer<Event>(
                this.TOPIC,
                new Event.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.appealProducer = new FlinkKafkaProducer<Appeal>(
                this.TOPIC,
                new Appeal.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.refuseProducer = new FlinkKafkaProducer<Refuse>(
                this.TOPIC,
                new Refuse.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return this;
    }

    // TODO: Sink "stuff" to Kafka
    public void sink() {

    }
}
