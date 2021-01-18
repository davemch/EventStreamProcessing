package analytics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import types.aggregate.AggregateAppealEvent;
import types.gdelt.Event;
import types.socialunrest.Refuse;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Aggregate {

    /**
     * Aggregate number of events per day
     */
    public static class AggregateEventsPerDay extends ProcessWindowFunction<Event, AggregateAppealEvent, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> key, Context context, Iterable<Event> events, Collector<AggregateAppealEvent> collector) throws Exception {
            int amount = 0;

            for (Event event : events) {
                amount++;
            }
            Date startDate = new Date(context.window().getStart());
            Date endDate = new Date(context.window().getEnd());

            collector.collect(new AggregateAppealEvent(startDate, endDate, amount));
        }
    }
}

