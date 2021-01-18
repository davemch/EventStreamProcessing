package types.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import types.socialunrest.Accusation;

import java.util.Date;

public class AggregateAccusationEvent extends AggregateEvent {

    public AggregateAccusationEvent(Date startDate, Date endDate, int amount) {
        super("accusation_aggregate", startDate, endDate, amount);
    }

    /**
     * Aggregator
     */
    public static class AggregateAccusationEvents extends ProcessWindowFunction<Accusation, AggregateAccusationEvent, Tuple2<String, Date>, TimeWindow> {
        @Override
        public void process(Tuple2<String, Date> key, Context context, Iterable<Accusation> events, Collector<AggregateAccusationEvent> collector) throws Exception {
            int amount = 0;

            for (Accusation event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateAccusationEvent(startDate, endDate, amount));
        }
    }

    /**
     * Kafka Serializer
     */
    public static class Serializer implements KeyedSerializationSchema<AggregateAccusationEvent> {
        @Override
        public byte[] serializeKey(AggregateAccusationEvent aggregateAccusationEvent) {
            return null;
        }

        @Override
        public byte[] serializeValue(AggregateAccusationEvent aggregateAccusationEvent) {
            return aggregateAccusationEvent.toString().getBytes();
        }

        @Override
        public String getTargetTopic(AggregateAccusationEvent aggregateAccusationEvent) {
            return null;
        }
    }
}
