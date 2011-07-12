package ness.cache;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;
import io.trumpet.log.Log;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.http.annotation.ThreadSafe;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

@Singleton
@ThreadSafe
class MemcachedClientFactory {
    private static final Log LOG = Log.findLog();
    private volatile MemcachedClient client;
    private volatile List<InetSocketAddress> memcachedCluster = ImmutableList.of();
    private final ScheduledExecutorService clientReconfigurationService;
    private final CacheTopologyProvider cacheTopology;
    
    @Inject
    MemcachedClientFactory(final CacheConfiguration configuration, Lifecycle lifecycle, CacheTopologyProvider cacheTopology) {
        
        this.cacheTopology = cacheTopology;
        clientReconfigurationService = new ScheduledThreadPoolExecutor(1, 
                new ThreadFactoryBuilder().setNameFormat("memcached-discovery-%d").build());
        
        lifecycle.addListener(LifecycleStage.START_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                LOG.info("Kicking off memcache topology discovery thread");
                MemcachedDiscoveryUpdate updater = new MemcachedDiscoveryUpdate();
                updater.run();
                clientReconfigurationService.scheduleAtFixedRate(
                        updater,
                        configuration.getCacheServerRediscoveryInterval(), 
                        configuration.getCacheServerRediscoveryInterval(), TimeUnit.MILLISECONDS);
            }
        });
        
        lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                clientReconfigurationService.shutdown();
                LOG.info("Caching system stopped");
            }
        });
    }
    
    // This method must be incredibly cheap
    public MemcachedClient get() {
        return client;
    }
    
    private class MemcachedDiscoveryUpdate implements Runnable {
        @Override
        public void run() {
            // Guaranteed to not run multiply; see ScheduledExecutorService.
            LOG.trace("Cache prior to discovery = %s", client);
            
            // Discover potentially new cluster topology
            List<InetSocketAddress> addrs = cacheTopology.get();
            
            if (!memcachedCluster.equals(addrs)) {
                // Need to copy to ensure thread safe sharing via a volatile
                memcachedCluster = ImmutableList.copyOf(addrs);
                
                try {
                    MemcachedClient newClient = new MemcachedClient(new BinaryConnectionFactory() {
                        @SuppressWarnings(value = {"unchecked", "rawtypes"} )
                        @Override
                        public Transcoder<Object> getDefaultTranscoder() {
                            return (Transcoder) new MemcacheByteArrayTranscoder();
                        }
                    }, addrs);
                    client = newClient;
                    LOG.debug("Discovery complete, new client talks to %s -> %s", addrs, newClient);
                } catch (IOException e) {
                    LOG.error(e, "Could not connect to memcached cluster");
                }
            } else {
                LOG.trace("No cache update due to identical cluster configuration %s", memcachedCluster);
            }
        }
    }
}
