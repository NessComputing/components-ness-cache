package ness.cache2;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

final class MemcacheByteArrayTranscoder implements Transcoder<byte[]> {
    @Override
    public boolean asyncDecode(CachedData d) {
        return false;
    }

    @Override
    public CachedData encode(byte[] data) {
        // Apparently Memcache does not differentiate between a byte[0] and null,
        // so use the flag field to mark nonnull values
        return new CachedData(data == null ? 0 : 1, data, getMaxSize());
    }

    @Override
    public byte[] decode(CachedData d) {
        return d.getFlags() == 0 ? null : d.getData();
    }

    @Override
    public int getMaxSize() {
        return Integer.MAX_VALUE;
    }
}
