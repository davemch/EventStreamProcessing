package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import types.SimpleEvent;

public class Filters {

    /**
     * SocialUnrestFilter filters SimpleEvents based on the `EventRootCode`-field.
     */
    public static class SocialUnrestFilter implements FilterFunction<SimpleEvent> {
        @Override
        public boolean filter(SimpleEvent event) throws Exception {
            String eventCode = event.getEventRootCode();

            String[] socialUnrestCodes = {
                    "10", // Demand
                    "11", // Disapprove
                    "12", // Reject
                    "13", // Threaten
                    "14"  // Protest
            };

            for (String want : socialUnrestCodes) {
                if (eventCode.equals(want)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * CountryFilter filters SimpleEvents based on the `ActionGeo_CountryCode`-field.
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
}
