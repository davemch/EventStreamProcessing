package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import types.SimpleEvent;

import java.util.Date;

public class Filters {

    /**
     * SocialUnrestFilter filters SimpleEvents based on the `EventRootCode`-field.
     */
    public static class SocialUnrestFilter implements FilterFunction<SimpleEvent> {
        @Override
        public boolean filter(SimpleEvent event) throws Exception {
            String eventCode = event.getEventRootCode();
            String want = "01";

            /*
            String[] socialUnrestCodes = {
                    "010", // Demand
                    "011", // Disapprove
                    "012", // Reject
                    "013", // Threaten
                    "014"  // Protest
            };
             */

            return eventCode.equals(want);
        }
    }

    /**
     * CountryFilter filters SimpleEvents based on the `A1CountryCode`-field and
     * the given country code.
     */
    public static class CountryFilter implements FilterFunction<SimpleEvent> {
        private final String want;

        public CountryFilter(String country) {
            this.want = country;
        }

        @Override
        public boolean filter(SimpleEvent event) throws Exception {
            return event.getA1CountryCode().equals(want);
        }
    }

    /**
     * TimeFilter filters SimpleEvents based on the `Date`-field and the given
     * time frame.
     */
    public static class TimeFilter implements FilterFunction<SimpleEvent> {
        private final Date from, to;

        public TimeFilter(Date from, Date to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean filter(SimpleEvent event) throws Exception {
            return event.getDate().after(from) && event.getDate().before(to);
        }
    }
}
