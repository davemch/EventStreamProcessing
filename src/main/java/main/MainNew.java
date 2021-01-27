package main;

import filter.Filters;
import kafka.Kafka;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import types.socialunrest.*;
import types.gdelt.Event;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import parser.LineParser;
import sources.ZipLoader;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Parameters to run:
 *   --input ./downloader/files/ (or wherever you downloaded GDELT's .zip-files to)
 */
public class MainNew {
    public static void main(String[] args) throws Exception {

        /**
         * TODO:
         *       * Amount of events per day
         *       * Amount of ... events per day
         *       * Moving average of events per day
         *       * Moving average of ... events per day
         */

        // Parse command line arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String filesDirectory = parameters.get("input");

        // Setup execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Setup Kafka
        Kafka kafka = new Kafka("gdelt", "bootstrap.servers", "localhost:9092")
                .initProducers();

        // Call ZipLoader to stream GDELTs .zip-files content line by line
        DataStream<String> rawLine = env.addSource(new ZipLoader(filesDirectory));

        // Parse GDELTs csv-line to the SimpleEvent POJO-type...
        DataStream<Event> eventStream = rawLine
                .flatMap(new LineParser())
                .assignTimestampsAndWatermarks(new Event.ExtractTimestamp())
                // ... and filter them
                .filter(new Filters.SocialUnrestFilter())
                .filter(new Filters.CountryFilter("USA"))
                // aggregate per day for all the event codes
                .keyBy(Event::getEventCode)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new MovingAverageFunction());

        eventStream.print();

        /* CEP Example 1. using a threshold for event 014 */
        /*
        DataStream<Event> eruptionEventStream = eventStream.filter(new FilterEruptionEvent());

        eruptionEventStream.print();

        // Check for Eruption Events
        PatternStream<Event> eruptionPatternStream = CEP.pattern(eruptionEventStream, patternWithThreshold());
        DataStream<Eruption> eruptionDataStream = eruptionPatternStream.select(new Eruption.Creator());

        eruptionDataStream.print();

         */  // end Example 1.


        // CEP Example 2.
        // Check if withing 5 days we have a sequence where
        // at start we have an event 010 (appeal)
        // in middle we have any of events 011 (accusation), 012 (refuse) or , 013 (Escalation)
        // in the end of the sequence there is an 014 (eruption) event

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getEventCode().contentEquals("010");
                    }
                }
        ).next("middl").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return (event.getEventCode().contentEquals("011") ||
                                event.getEventCode().contentEquals("012") ||
                                event.getEventCode().contentEquals("013"));
                    }
                }
        ).timesOrMore(10).followedBy("eeend").where(
        //).oneOrMore().followedBy("eeend").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getEventCode().contentEquals("014");
                    }
                }
        ).within(Time.days(5));


        PatternStream<Event> patternStream = CEP.pattern(eventStream, pattern);

        // Tuple2<date,eventCode>
        DataStream<Tuple2<String,String>> alertResult = patternStream.process(
                new PatternProcessFunction<Event, Tuple2<String,String>>() {
                    @Override
                    public void processMatch(Map<String, List<Event>> pattern,
                            Context ctx,
                            Collector<Tuple2<String,String>> out) throws Exception {

                        System.out.println("   Map size: " + pattern.size());
                        for (Map.Entry<String, List<Event>> entry : pattern.entrySet()) {
                            System.out.println("   " + entry.getKey() + " --> [" + entry.getValue().size() + "]  " +  entry.getValue());
                        }
                        System.out.println();

                        out.collect(new Tuple2<String,String>("WARNING",pattern.toString()));
                    }
                });

        alertResult.print();

         // end Example 2.

        // Execute environment
        env.execute("GDELT: Black-Lives-Matter");
    }

    public static class FilterEruptionEvent implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if(event.getEventCode().contentEquals("014") )
                return true;
            else
                return false;
        }
    }

    public static Pattern<Event, ?> patternWithThreshold() {
        return Pattern.<Event>begin("first").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        if(event.getAvgTone() > 4.0)  // NOTE: AvgTone is actually avgNumMentions !!! see NOTE in MainNew.MovingAverageFunction
                            return true;
                        else
                            return false;
                    }
                }
        ).timesOrMore(3)
                .within(Time.days(3));


    }

    public static class MovingAverageFunction extends ProcessWindowFunction<Event, Event, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Event> iterable, Collector<Event> collector) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            int count = 0;
            int mentions = 0;
            double avgMentions = 0.0;
            double avgTone = 0.0;
            int n=0;
            for (Event in: iterable) {
                mentions = mentions + in.getNumMentions();
                count++;
                //System.out.println("    LINE: " + in.getA1CountryCode() + "  " + key + "  " + sdf.format(in.getDate()) + "  " + in.getEventRootCode() + "  " + in.getNumMentions() + " count=" + count);
            }
            if(count>0) {
                avgMentions = mentions / (1.0*count);
                avgTone = avgTone / (1.0*count);
            }
            // a window has: context.window().getStart() and context.window().getEnd()
            // here it is return the end time of the window

            Date end = new Date(context.window().getEnd());
            String countryKey = iterable.iterator().next().getA1CountryCode();
            String rootCode = iterable.iterator().next().getEventRootCode();

            // return an Event
            // NOTE: just for testing!  I am returning the average num mentions in the place of avgTone...(because I need a double)
            Event aggregatedEvent =
                    new Event(end, countryKey, key, rootCode, mentions, avgMentions, 0.0,0.0);
            collector.collect(aggregatedEvent);
        }
    }



}

