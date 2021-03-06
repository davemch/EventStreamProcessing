package filter;

import org.apache.flink.api.common.functions.FilterFunction;
import types.gdelt.Event;

import java.util.Date;

public class Filters {

    /**
     * SocialUnrestFilter filters SimpleEvents based on the `EventRootCode`-field.
     */
    public static class SocialUnrestFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            String eventCode = event.getEventRootCode();
            String want = "01";

            return eventCode.equals(want);
        }
    }

    /**
     * CountryFilter filters Events based on the `A1CountryCode`-field and
     * the given country code.
     */
    public static class CountryFilter implements FilterFunction<Event> {
        private final String want;

        public CountryFilter(String country) {
            this.want = country;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.getA1CountryCode().equals(want);
        }
    }

    /**
     * TODO: This is unused as the downloader tool downloads the required timeframe.
     *       We leave it here for possible future use.
     *
     * TimeFilter filters SimpleEvents based on the `Date`-field and the given
     * time frame.
     */
    public static class TimeFilter implements FilterFunction<Event> {
        private final Date start, end;

        public TimeFilter(Date start, Date end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.getDate().after(start) && event.getDate().before(end);
        }
    }
}
