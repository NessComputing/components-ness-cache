package ness.cache;

import java.net.URI;
import java.util.List;

import org.skife.config.Config;
import org.skife.config.Default;
import org.skife.config.DefaultNull;

public abstract class CacheConfiguration {
    
    public enum CacheType {
        NONE, JVM, MEMCACHE
    }
    
    @Config("ness.cache")
    @Default("none")
    public CacheType getCacheType() {
        return CacheType.NONE;
    }
    
    /**
     * @return the period between updating the cache server topology information from service discovery, in milliseconds
     */
    @Config("ness.cache.rediscover-interval")
    @Default("1000")
    public long getCacheServerRediscoveryInterval() {
        return 1000;
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
}
