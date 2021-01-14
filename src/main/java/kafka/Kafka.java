package kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import types.base.*;
import types.base.gdelt.Event;

import java.util.Properties;

public class Kafka {
    private final String TOPIC;
    private final Properties properties = new Properties();

    // -- Producers
    // GDELT Event
    public FlinkKafkaProducer<Event> simpleEventProducer;
    // Basic SocialUnrestEvents
    public FlinkKafkaProducer<Appeal>     appealProducer;
    public FlinkKafkaProducer<Accusation> accusationProducer;
    public FlinkKafkaProducer<Refuse>     refuseProducer;
    public FlinkKafkaProducer<Escalation> escalationProducer;
    public FlinkKafkaProducer<Eruption>   eruptionProducer;

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

        this.accusationProducer = new FlinkKafkaProducer<Accusation>(
                this.TOPIC,
                new Accusation.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.refuseProducer = new FlinkKafkaProducer<Refuse>(
                this.TOPIC,
                new Refuse.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.escalationProducer = new FlinkKafkaProducer<Escalation>(
                this.TOPIC,
                new Escalation.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.eruptionProducer = new FlinkKafkaProducer<Eruption>(
                this.TOPIC,
                new Eruption.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return this;
    }

    // TODO: Sink "stuff" to Kafka
    public void sink() {

    }
}
