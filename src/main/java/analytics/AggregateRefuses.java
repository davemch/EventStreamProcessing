package analytics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import types.base.Refuse;

import java.util.Date;

public class AggregateRefuses {

    /**
     * Aggregate number of refuses per day
     */
    public static class AggregateRefusesPerDay extends ProcessWindowFunction<Refuse, Tuple2<Date, Integer>, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<Refuse> refuses, Collector<Tuple2<Date, Integer>> collector) throws Exception {
            int amount = 0;

            for (Refuse refuse : refuses) {

            }
        }
    }
}

