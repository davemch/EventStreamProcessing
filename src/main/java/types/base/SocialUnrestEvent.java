package types.base;

import java.util.Date;

public abstract class SocialUnrestEvent {
    final String eventCode;
    final Date date;

    public SocialUnrestEvent(String eventCode, Date date) {
        this.eventCode = eventCode;
        this.date = date;
    }

    @Override
    public abstract String toString();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);
}
