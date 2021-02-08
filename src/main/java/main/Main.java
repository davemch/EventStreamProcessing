package main;

import filter.Filters;
import kafka.Kafka;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import patterns.Patterns;
import types.aggregate.*;
import types.socialunrest.*;
import types.gdelt.Event;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import parser.LineParser;
import sources.ZipLoader;

import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

/**
 * Parameters to run:
 *   --input ./downloader/files/ (or wherever you downloaded GDELT's .zip-files to)
 *   --debug [optional, default is false] {true|false}
 *          If set to true all produced events will be printed to Stdout.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        boolean debug;

        /*
         * Setup
         */

        // Parse command line arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        String filesDirectory = parameters.get("input");
        debug = Boolean.parseBoolean(parameters.get("debug"));

        // Setup execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
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
                .filter(new Filters.CountryFilter("USA"));


        /*
         * Check for basic SocialUnrestEvents
         */

        // Check for Appeal Events
        PatternStream<Event> appealPatternStream = CEP.pattern(eventStream, Appeal.getPattern());
        DataStream<Appeal> appealDataStream = appealPatternStream.select(new Appeal.Creator());
        //appealDataStream.addSink(kafka.appealProducer);
        if (debug)
            appealDataStream.print();

        // Check for Accusation Events
        PatternStream<Event> accusationPatternStream = CEP.pattern(eventStream, Accusation.getPattern());
        DataStream<Accusation> accusationDataStream = accusationPatternStream.select(new Accusation.Creator());
        //accusationDataStream.addSink(kafka.accusationProducer);
        if (debug)
            accusationDataStream.print();

        // Check for Refuse Events
        PatternStream<Event> refusePatternStream = CEP.pattern(eventStream, Refuse.getPattern());
        DataStream<Refuse> refuseDataStream = refusePatternStream.select(new Refuse.Creator());
        //refuseDataStream.addSink(kafka.refuseProducer);
        if (debug)
            refuseDataStream.print();

        // Check for Escalation Events
        PatternStream<Event> escalationPatternStream = CEP.pattern(eventStream, Escalation.getPattern());
        DataStream<Escalation> escalationDataStream = escalationPatternStream.select(new Escalation.Creator());
        //escalationDataStream.addSink(kafka.escalationProducer);
        if (debug)
            escalationDataStream.print();

        // Check for Eruption Events
        PatternStream<Event> eruptionPatternStream = CEP.pattern(eventStream, Eruption.getPattern());
        DataStream<Eruption> eruptionDataStream = eruptionPatternStream.select(new Eruption.Creator());
        //eruptionDataStream.addSink(kafka.eruptionProducer);
        if (debug)
            eruptionDataStream.print();


        /*
         * Calculate the amount of SocialUnrestEvents per week
         */
        final int AMOUNT_DAYS = 7;

        // Amount of Appeal events
        DataStream<AggregateAppealEvent> aggregateAppealEventDataStream = appealDataStream
                .keyBy(new Appeal.AppealKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(AMOUNT_DAYS)))
                .process(new AggregateAppealEvent.AggregateAppealEvents());
        aggregateAppealEventDataStream.addSink(kafka.aggregateAppealProducer);
        if (debug)
            aggregateAppealEventDataStream.print();

        // Amount of Accusation events
        DataStream<AggregateAccusationEvent> aggregateAccusationEventDataStream = accusationDataStream
                .keyBy(new Accusation.AccusationKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(AMOUNT_DAYS)))
                .process(new AggregateAccusationEvent.AggregateAccusationEvents());
        aggregateAccusationEventDataStream.addSink(kafka.aggregateAccusationProducer);
        if (debug)
            aggregateAccusationEventDataStream.print();

        // Amount of Refuse events
        DataStream<AggregateRefuseEvent> aggregateRefuseEventDataStream = refuseDataStream
                .keyBy(new Refuse.RefuseKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(AMOUNT_DAYS)))
                .process(new AggregateRefuseEvent.AggregateRefuseEvents());
        aggregateRefuseEventDataStream.addSink(kafka.aggregateRefuseProducer);
        if (debug)
            aggregateRefuseEventDataStream.print();

        // Amount of Escalation events
        DataStream<AggregateEscalationEvent> aggregateEscalationEventDataStream = escalationDataStream
                .keyBy(new Escalation.EscalationKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(AMOUNT_DAYS)))
                .process(new AggregateEscalationEvent.AggregateEscalationEvents());
        aggregateEscalationEventDataStream.addSink(kafka.aggregateEscalationProducer);
        if (debug)
            aggregateEscalationEventDataStream.print();

        // Amount of Eruption events
        DataStream<AggregateEruptionEvent> aggregateEruptionEventDataStream = eruptionDataStream
                .keyBy(new Eruption.EruptionKeySelector())
                .window(TumblingEventTimeWindows.of(Time.days(AMOUNT_DAYS)))
                .process(new AggregateEruptionEvent.AggregateEruptionEvents());
        aggregateEruptionEventDataStream.addSink(kafka.aggregateEruptionProducer);
        if (debug)
            aggregateEruptionEventDataStream.print();






        /*
         * Check for increase in SocialUnrestEvents
         */

        // Check for Appeal amount increase
        PatternStream<AggregateAppealEvent> aggregateAppealEventPatternStream = CEP.pattern(
                aggregateAppealEventDataStream,
                Patterns.APPEAL_THRESHOLD_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> appealWarning = aggregateAppealEventPatternStream.process(
                new PatternProcessFunction<AggregateAppealEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<AggregateAppealEvent>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        // NOTE: Divide getTime() by 1000 to convert milliseconds to seconds.
                        out.collect(new Tuple3<String, Long, Long>("appeal_WARNING",
                                pattern.get("first").get(0).getDate().getTime() / 1000,
                                pattern.get("second").get(0).getDate().getTime() / 1000));
                    }
                }
        );
        appealWarning.addSink(kafka.warningProducer);
        if (debug)
            appealWarning.print();

        // Check for Accusation amount increase
        PatternStream<AggregateAccusationEvent> aggregateAccusationEventPatternStream = CEP.pattern(
                aggregateAccusationEventDataStream,
                Patterns.ACCUSATION_THRESHOLD_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> accusationWarning = aggregateAccusationEventPatternStream.process(
                new PatternProcessFunction<AggregateAccusationEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<AggregateAccusationEvent>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(new Tuple3<String, Long, Long>("accusation_WARNING",
                                pattern.get("first").get(0).getDate().getTime() / 1000,
                                pattern.get("second").get(0).getDate().getTime() / 1000));
                    }
                }
        );
        accusationWarning.addSink(kafka.warningProducer);
        if (debug)
            accusationWarning.print();

        // Check for Refuse amount increase
        PatternStream<AggregateRefuseEvent> aggregateRefuseEventPatternStream = CEP.pattern(
                aggregateRefuseEventDataStream,
                Patterns.REFUSE_THRESHOLD_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> refuseWarning = aggregateRefuseEventPatternStream.process(
                new PatternProcessFunction<AggregateRefuseEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<AggregateRefuseEvent>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(new Tuple3<String, Long, Long>("refuse_WARNING",
                                pattern.get("first").get(0).getDate().getTime() / 1000,
                                pattern.get("second").get(0).getDate().getTime() / 1000));
                    }
                }
        );
        refuseWarning.addSink(kafka.warningProducer);
        if (debug)
            refuseWarning.print();

        // Check for Escalation amount increase
        PatternStream<AggregateEscalationEvent> aggregateEscalationEventPatternStream = CEP.pattern(
                aggregateEscalationEventDataStream,
                Patterns.ESCALATION_THRESHOLD_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> escalationWarning = aggregateEscalationEventPatternStream.process(
                new PatternProcessFunction<AggregateEscalationEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<AggregateEscalationEvent>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(new Tuple3<String, Long, Long>("escalation_WARNING",
                                pattern.get("first").get(0).getDate().getTime() / 1000,
                                pattern.get("second").get(0).getDate().getTime() / 1000));
                    }
                }
        );
        escalationWarning.addSink(kafka.warningProducer);
        if (debug)
            escalationWarning.print();

        // Check for Eruption amount increase
        PatternStream<AggregateEruptionEvent> aggregateEruptionEventPatternStream = CEP.pattern(
                aggregateEruptionEventDataStream,
                Patterns.ERUPTION_THRESHOLD_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> eruptionWarning = aggregateEruptionEventPatternStream.process(
                new PatternProcessFunction<AggregateEruptionEvent, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<AggregateEruptionEvent>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(new Tuple3<String, Long, Long>("eruption_WARNING",
                                pattern.get("first").get(0).getDate().getTime() / 1000,
                                pattern.get("second").get(0).getDate().getTime() / 1000));
                    }
                }
        );
        eruptionWarning.addSink(kafka.warningProducer);
        if (debug)
            eruptionWarning.print();


        /*
         * Check for warning events
         */

        // Merge Warning DataStreams
        DataStream<Tuple3<String, Long, Long>> singleWarnings = appealWarning
                .union(accusationWarning)
                .union(refuseWarning)
                .union(escalationWarning)
                .union(eruptionWarning);

        // Check for alert event patterns
        PatternStream<Tuple3<String, Long, Long>> warningsPatternStream = CEP.pattern(
                singleWarnings,
                Patterns.ALERT_PATTERN
        );
        DataStream<Tuple3<String, Long, Long>> complexWarnings = warningsPatternStream.process(
                new PatternProcessFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>>() {
                    @Override
                    public void processMatch(Map<String, List<Tuple3<String, Long, Long>>> pattern,
                                             Context ctx,
                                             Collector<Tuple3<String, Long, Long>> out) throws Exception {
                        out.collect(new Tuple3<String, Long, Long>(
                                "ALERT",
                                Long.divideUnsigned(pattern.get("first").get(0).f1, 1000),
                                Long.divideUnsigned(pattern.get("first").get(0).f2, 1000)));
                    }
                }
        );
        complexWarnings.addSink(kafka.warningProducer);
        if (debug)
            complexWarnings.print();



        /*
         * Execute environment
         */
        env.execute("GDELT: Black-Lives-Matter");
    }
}
