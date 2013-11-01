package com.nesscomputing.cache;

import java.net.InetSocketAddress;
import java.util.Comparator;

import com.google.common.base.Preconditions;

class InetSocketAddressComparator implements Comparator<InetSocketAddress>
{
    public static final InetSocketAddressComparator DEFAULT = new InetSocketAddressComparator();

    private InetSocketAddressComparator()
    {
    }

    @Override
    public int compare(final InetSocketAddress o1, final InetSocketAddress o2)
    {
        Preconditions.checkNotNull(o1);
        Preconditions.checkNotNull(o2);
        return o1.toString().compareTo(o2.toString()); // Yeah, it is cheap.
    }
}
