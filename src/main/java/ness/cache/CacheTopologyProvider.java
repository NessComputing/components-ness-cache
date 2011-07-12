package ness.cache;

import io.trumpet.log.Log;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import ness.discovery.client.ReadOnlyDiscoveryClient;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
class CacheTopologyProvider {
    private static final Log LOG = Log.findLog();
    private final List<InetSocketAddress> addrs;
    private final ReadOnlyDiscoveryClient discoveryClient;
    
    @Inject
    CacheTopologyProvider(CacheConfiguration config, ReadOnlyDiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
        List<URI> uris = config.getCacheUri();
        if (uris != null) {
            ImmutableList.Builder<InetSocketAddress> addrBuilder = ImmutableList.builder();
            for (URI uri : uris) {
                if ("memcache".equals(uri.getScheme())) {
                    addrBuilder.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
                } else {
                    LOG.warn("Ignored uri %s due to wrong scheme", uri);
                }
            }
            addrs = addrBuilder.build();
        } else {
            addrs = null;
        }
    }
    
    public List<InetSocketAddress> get() {
        if (addrs != null) {
            return addrs;
        }
        // TODO
        discoveryClient.findAllServiceInformation("memcached");
        return Collections.singletonList(new InetSocketAddress("localhost", 11211));
    }
}
