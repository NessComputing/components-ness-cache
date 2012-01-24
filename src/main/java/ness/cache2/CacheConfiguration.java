package ness.cache2;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;
import org.skife.config.TimeSpan;

public abstract class CacheConfiguration {

    public enum CacheType {
        /** No caching is done; all reads return no results */
        NONE,
        /** In-JVM caching only */
        JVM,
        /** In-JVM. Does not evict keys when low on memory (you will get an OOM error) */
        JVM_NO_EVICTION,
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
    @Config({"ness.cache.${cacheName}", "ness.cache"})
    @Default("NONE")
    public CacheType getCacheType() {
        return CacheType.NONE;
    }

    /**
     * Specifies which encoding/decoding scheme all memcached caching should apply to namespaces
     * and keys before passing them to the memcached client.  The currently supported values are
     * BASE64 and NONE, defaulting to BASE64.  Only use NONE if you -know- that none of the
     * namespaces and keys will contain any bytes prohibited by memcached (spaces, newlines,
     * carriage returns and nulls).
     *
     * @return the desired ns/key encoding within the memcached provider
     */
    @Config({"ness.cache.${cacheName}.memcached-encoding", "ness.cache.memcached-encoding"})
    @Default("BASE64")
    public EncodingType getMemcachedEncoding() {
        return EncodingType.BASE64;
    }

    /**
     * @return the period between updating the cache server topology information from service discovery, in milliseconds
     */
    @Config({"ness.cache.${cacheName}.rediscover-interval", "ness.cache.rediscover-interval"})
    @Default("1000ms")
    public TimeSpan getCacheServerRediscoveryInterval() {
        return new TimeSpan(1000, TimeUnit.MILLISECONDS);
    }

    /**
     * @return whether the cache should wait for set and clear operations to report success before proceeding forward
     */
    @Config({"ness.cache.${cacheName}.synchronous", "ness.cache.synchronous"})
    @Default("false")
    public boolean isCacheSynchronous() {
        return false;
    }

    /**
     * @return the cache locations to use; overrides and disables discovery.
     */
    @Config({"ness.cache.${cacheName}.uri", "ness.cache.uri"})
    @DefaultNull
    public List<URI> getCacheUri() {
        return null;
    }

    /**
     * @return whether cache JMX exporting is enabled
     */
    @Config({"ness.cache.${cacheName}.jmx", "ness.cache.jmx"})
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

    public static final CacheConfiguration IN_JVM_NO_EVICTION = new CacheConfiguration() {
        @Override
        public CacheType getCacheType() {
            return CacheType.JVM_NO_EVICTION;
        }
    };

    //
    // Everything below here is memcache specific.
    //

    /**
     * Number of node repetitions in the ketama consistent hash ring.
     */
    @Config({"ness.cache.${cacheName}.node-repetitions", "ness.cache.node-repetitions"})
    @Default("160")
    public int getNodeRepetitions() {
        return 160;
    }

    @Config({"ness.cache.${cacheName}.read-queue", "ness.cache.read-queue"})
    @Default("32768")
    public int getReadQueueSize() {
        return 32768;
    }

    @Config({"ness.cache.${cacheName}.write-queue", "ness.cache.write-queue"})
    @Default("32768")
    public int getWriteQueueSize() {
        return 32768;
    }

    @Config({"ness.cache.${cacheName}.incoming-queue", "ness.cache.incoming-queue"})
    @Default("16384")
    public int getIncomingQueueSize() {
        return 16384;
    }

    @Config({"ness.cache.${cacheName}.read-buffer-size", "ness.cache.read-buffer-size"})
    @Default("16384")
    public int getReadBufferSize() {
        return 16384;
    }

    @Config({"ness.cache.${cacheName}.op-max-block-time", "ness.cache.op-max-block-time"})
    @Default("100ms")
    public TimeSpan getOperationQueueBlockTime() {
        return new TimeSpan(100, TimeUnit.MILLISECONDS);
    }

    @Config({"ness.cache.${cacheName}.operation-timeout", "ness.cache.operation-timeout"})
    @Default("2500ms")
    public TimeSpan getOperationTimeout() {
        return new TimeSpan(2500, TimeUnit.MILLISECONDS);
    }

    @Config({"ness.cache.${cacheName}.daemon-threads", "ness.cache.daemon-threads"})
    @Default("false")
    public boolean isDaemonThreads() {
        return false;
    }
}
