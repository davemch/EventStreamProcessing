package patterns;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;
import types.aggregate.*;
import types.gdelt.Event;
import types.socialunrest.Appeal;

import java.util.ArrayList;
import java.util.Date;

/**
 * Provides patterns to match for in complex event processing.
 */
public class Patterns {

    /**
     * Appeal 7-day average increased by >10%
     */
    private static double APPEAL_LAST_AMOUNT; // NOTE: We use different variables for each pattern as we are not sure
    private static Date APPEAL_LAST_DATE;     //       if we would run into race conditions otherwise.
    public final static Pattern<AggregateAppealEvent, ?> APPEAL_THRESHOLD_PATTERN =
            Pattern.<AggregateAppealEvent>begin("first").where(
                    new SimpleCondition<AggregateAppealEvent>() {
                        @Override
                        public boolean filter(AggregateAppealEvent aggregateAppealEvent) throws Exception {
                            APPEAL_LAST_AMOUNT = aggregateAppealEvent.getAmount();
                            APPEAL_LAST_DATE = aggregateAppealEvent.getDate();
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<AggregateAppealEvent>() {
                        @Override
                        public boolean filter(AggregateAppealEvent in) throws Exception {
                            // Check if same date
                            if (APPEAL_LAST_DATE.equals(in.getDate()))
                                return false;

                            double change = in.getAmount() / APPEAL_LAST_AMOUNT;
                            return change > 1.10; // Increase by >10%
                        }
                    });

    /**
     * Accusation 7-day average increased by >60%
     */
    private static double ACCUSATION_LAST_AMOUNT;
    private static Date ACCUSATION_LAST_DATE;
    public final static Pattern<AggregateAccusationEvent, ?> ACCUSATION_THRESHOLD_PATTERN =
            Pattern.<AggregateAccusationEvent>begin("first").where(
                    new SimpleCondition<AggregateAccusationEvent>() {
                        @Override
                        public boolean filter(AggregateAccusationEvent aggregateAccusationEvent) throws Exception {
                            ACCUSATION_LAST_AMOUNT = aggregateAccusationEvent.getAmount();
                            ACCUSATION_LAST_DATE = aggregateAccusationEvent.getDate();
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<AggregateAccusationEvent>() {
                        @Override
                        public boolean filter(AggregateAccusationEvent in) throws Exception {
                            // Check if same date
                            if (ACCUSATION_LAST_DATE.equals(in.getDate()))
                                return false;

                            double change = in.getAmount() / ACCUSATION_LAST_AMOUNT;
                            return change > 1.60; // Increase by >60%
                        }
                    });

    /**
     * Refuse 7-day average increased by >10%
     */
    private static double REFUSE_LAST_AMOUNT;
    private static Date REFUSE_LAST_DATE;
    public final static Pattern<AggregateRefuseEvent, ?> REFUSE_THRESHOLD_PATTERN =
            Pattern.<AggregateRefuseEvent>begin("first").where(
                    new SimpleCondition<AggregateRefuseEvent>() {
                        @Override
                        public boolean filter(AggregateRefuseEvent in) throws Exception {
                            REFUSE_LAST_AMOUNT = in.getAmount();
                            REFUSE_LAST_DATE = in.getDate();
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<AggregateRefuseEvent>() {
                        @Override
                        public boolean filter(AggregateRefuseEvent in) throws Exception {
                            // Check if same date
                            if (REFUSE_LAST_DATE.equals(in.getDate()))
                                return false;

                            double change = in.getAmount() / REFUSE_LAST_AMOUNT;
                            return change > 1.10; // Increase by >10%
                        }
                    });

    /**
     * Escalation 7-day average increased by >10%
     */
    private static double ESCALATION_LAST_AMOUNT;
    private static Date ESCALATION_LAST_DATE;
    public final static Pattern<AggregateEscalationEvent, ?> ESCALATION_THRESHOLD_PATTERN =
            Pattern.<AggregateEscalationEvent>begin("first").where(
                    new SimpleCondition<AggregateEscalationEvent>() {
                        @Override
                        public boolean filter(AggregateEscalationEvent in) throws Exception {
                            ESCALATION_LAST_AMOUNT = in.getAmount();
                            ESCALATION_LAST_DATE = in.getDate();
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<AggregateEscalationEvent>() {
                        @Override
                        public boolean filter(AggregateEscalationEvent in) throws Exception {
                            // Check if same date
                            if (ESCALATION_LAST_DATE.equals(in.getDate()))
                                return false;

                            double change = in.getAmount() / ESCALATION_LAST_AMOUNT;
                            return change > 1.10; // Increase by >10%
                        }
                    });

    /**
     * Eruption 7-day average increased by >20%
     */
    private static double ERUPTION_LAST_AMOUNT;
    private static Date ERUPTION_LAST_DATE;
    public final static Pattern<AggregateEruptionEvent, ?> ERUPTION_THRESHOLD_PATTERN =
            Pattern.<AggregateEruptionEvent>begin("first").where(
                    new SimpleCondition<AggregateEruptionEvent>() {
                        @Override
                        public boolean filter(AggregateEruptionEvent in) throws Exception {
                            ERUPTION_LAST_AMOUNT = in.getAmount();
                            ERUPTION_LAST_DATE = in.getDate();
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<AggregateEruptionEvent>() {
                        @Override
                        public boolean filter(AggregateEruptionEvent in) throws Exception {
                            // Check if same date
                            if (ERUPTION_LAST_DATE.equals(in.getDate()))
                                return false;

                            double change = in.getAmount() / ERUPTION_LAST_AMOUNT;
                            return change > 1.20; // Increase by >20%
                        }
                    });

    /**
     * Three warnings appeared in same week
     */
    private static Long ALERT_LAST_DATE_START;
    private static Long ALERT_LAST_DATE_END;
    public final static Pattern<Tuple3<String, Long, Long>, ?> ALERT_PATTERN =
            Pattern.<Tuple3<String, Long, Long>>begin("first").where(
                    new SimpleCondition<Tuple3<String, Long, Long>>() {
                        @Override
                        public boolean filter(Tuple3<String, Long, Long> in) throws Exception {
                            ALERT_LAST_DATE_START = in.f1;
                            ALERT_LAST_DATE_END = in.f2;
                            return true;
                        }
                    }
            ).next("second").where(
                    new SimpleCondition<Tuple3<String, Long, Long>>() {
                        @Override
                        public boolean filter(Tuple3<String, Long, Long> in) throws Exception {
                            // Check timeframe
                            return (ALERT_LAST_DATE_START.equals(in.f1) && ALERT_LAST_DATE_END.equals(in.f2));
                        }
                    }
            ).next("third").where(
                    new SimpleCondition<Tuple3<String, Long, Long>>() {
                        @Override
                        public boolean filter(Tuple3<String, Long, Long> in) throws Exception {
                            // Check timeframe
                            return (ALERT_LAST_DATE_START.equals(in.f1) && ALERT_LAST_DATE_END.equals(in.f2));
                        }
                    });
}
