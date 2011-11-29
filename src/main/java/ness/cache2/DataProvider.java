package ness.cache2;

import org.joda.time.DateTime;

public interface DataProvider<U>
{
    DateTime getExpiry();

    U getData();
}

