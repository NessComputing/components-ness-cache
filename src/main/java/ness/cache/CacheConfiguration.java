package ness.cache;

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
        NONE, none,
        /** In-JVM caching only */
        JVM, jvm,
        /** External Memcache server */
        MEMCACHE, memcache
    }

    /**
     * @return the requested type of caching
     */
    @Config("ness.cache")
    @Default("none")
    public CacheType getCacheType() {
        return CacheType.NONE;
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
    public static final CacheConfiguration IN_JVM = new CacheConfiguration() { 
        @Override
        public CacheType getCacheType() {
            return CacheType.JVM;
        }
    };
}
