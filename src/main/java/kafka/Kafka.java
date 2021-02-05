package kafka;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import types.Alert;
import types.aggregate.*;
import types.socialunrest.*;

import java.util.Properties;

public class Kafka {
    private final String TOPIC;
    private final Properties properties = new Properties();

    // -- Producers

    // Basic SocialUnrestEvents
    public FlinkKafkaProducer<Appeal>     appealProducer;
    public FlinkKafkaProducer<Accusation> accusationProducer;
    public FlinkKafkaProducer<Refuse>     refuseProducer;
    public FlinkKafkaProducer<Escalation> escalationProducer;
    public FlinkKafkaProducer<Eruption>   eruptionProducer;

    // Aggregate Events
    public FlinkKafkaProducer<AggregateAppealEvent>     aggregateAppealProducer;
    public FlinkKafkaProducer<AggregateAccusationEvent> aggregateAccusationProducer;
    public FlinkKafkaProducer<AggregateRefuseEvent>     aggregateRefuseProducer;
    public FlinkKafkaProducer<AggregateEscalationEvent> aggregateEscalationProducer;
    public FlinkKafkaProducer<AggregateEruptionEvent>   aggregateEruptionProducer;

    // Warning Events
    public FlinkKafkaProducer<Tuple3<String, Long, Long>> warningProducer;

    public Kafka(String topic, String serverName, String serverLocation) {
        this.TOPIC = topic;
        this.properties.setProperty(serverName, serverLocation);
    }

    public Kafka initProducers() {
        // Basic SocialUnrestEvents
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

        // Aggregate Events
        this.aggregateAppealProducer = new FlinkKafkaProducer<AggregateAppealEvent>(
                this.TOPIC,
                new AggregateAppealEvent.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.aggregateAccusationProducer = new FlinkKafkaProducer<AggregateAccusationEvent>(
                this.TOPIC,
                new AggregateAccusationEvent.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.aggregateRefuseProducer = new FlinkKafkaProducer<AggregateRefuseEvent>(
                this.TOPIC,
                new AggregateRefuseEvent.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.aggregateEscalationProducer = new FlinkKafkaProducer<AggregateEscalationEvent>(
                this.TOPIC,
                new AggregateEscalationEvent.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        this.aggregateEruptionProducer = new FlinkKafkaProducer<AggregateEruptionEvent>(
                this.TOPIC,
                new AggregateEruptionEvent.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        // Warning Events
        this.warningProducer = new FlinkKafkaProducer<Tuple3<String, Long, Long>>(
                this.TOPIC,
                new Alert.Serializer(),
                this.properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        return this;
    }
}
