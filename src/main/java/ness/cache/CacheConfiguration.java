package ness.cache;

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;
import org.skife.config.TimeSpan;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class CacheConfiguration {

    public enum CacheType {
        /** No caching is done; all reads return no results */
        NONE,
        /** In-JVM caching only */
        JVM,
        /** External Memcache server */
        MEMCACHE
    }

    public enum EncodingType {
        NONE,
        BASE64
    }
    /**
     * @return the requested type of caching
     */
    @Config("ness.cache")
    @Default("NONE")
    public CacheType getCacheType() {
        return CacheType.NONE;
    }

    @Config("ness.cache.memcached-encoding")
    @Default("BASE64")
    public EncodingType getMemcachedEncoding() {
        return EncodingType.BASE64;
    }

    /**
     * @return the period between updating the cache server topology information from service discovery, in milliseconds
     */
    @Config("ness.cache.rediscover-interval")
    @Default("1000ms")
    public TimeSpan getCacheServerRediscoveryInterval() {
        return new TimeSpan(1000, TimeUnit.MILLISECONDS);
    }

    /**
     * @return whether the cache should wait for set and clear operations to report success before proceeding forward
     */
    @Config("ness.cache.synchronous")
    @Default("false")
    public boolean isCacheSynchronous() {
        return false;
    }

    /**
     * @return the cache locations to use; overrides and disables discovery.
     */
    @Config("ness.cache.uri")
    @DefaultNull
    public List<URI> getCacheUri() {
        return null;
    }

    /**
     * @return whether cache JMX exporting is enabled
     */
    @Config("ness.cache.jmx")
    @Default("true")
    public boolean isJmxEnabled() {
        return true;
    }

    public static final CacheConfiguration NONE = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.NONE;
        }
    };

    public static final CacheConfiguration NONE_NO_JMX = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.NONE;
        }
        @Override
        public boolean isJmxEnabled() {
            return false;
        }
    };

    public static final CacheConfiguration IN_JVM = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.JVM;
        }
    };

    @Config("ness.cache.read-queue")
    @Default("1000")
    public int getReadQueueSize() {
        return 1000;
    }

    @Config("ness.cache.write-queue")
    @Default("10000")
    public int getWriteQueueSize() {
        return 10000;
    }

    @Config("ness.cache.incoming-queue")
    @Default("1000")
    public int getIncomingQueueSize() {
        return 1000;
    }

    @Config("ness.cache.op-max-block-time")
    @Default("100ms")
    public TimeSpan getOperationQueueBlockTime() {
        return new TimeSpan(100, TimeUnit.MILLISECONDS);
    }
}
