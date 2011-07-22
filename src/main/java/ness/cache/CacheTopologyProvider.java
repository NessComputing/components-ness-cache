package ness.cache;

import io.trumpet.log.Log;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Set;

import ness.discovery.client.ReadOnlyDiscoveryClient;
import ness.discovery.client.ServiceInformation;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Use service discovery or configuration to assemble a set of Memcache servers dynamically.
 */
@Singleton
class CacheTopologyProvider {
    private static final Log LOG = Log.findLog();
    private final Set<InetSocketAddress> addrs;
    private final ReadOnlyDiscoveryClient discoveryClient;

    @Inject
    CacheTopologyProvider(CacheConfiguration config, ReadOnlyDiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;

        List<URI> uris = config.getCacheUri();
        if (uris != null) {
            ImmutableSet.Builder<InetSocketAddress> addrBuilder = ImmutableSet.builder();
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

    public Set<InetSocketAddress> get() {
        if (addrs != null) {
            return addrs;
        }

        return ImmutableSet.copyOf(Collections2.transform(discoveryClient.findAllServiceInformation("memcached"), new Function<ServiceInformation, InetSocketAddress>() {
            @Override
            public InetSocketAddress apply(ServiceInformation input) {
                return new InetSocketAddress(input.getProperty(ServiceInformation.PROP_SERVICE_ADDRESS), 
                        Integer.valueOf(input.getProperty(ServiceInformation.PROP_SERVICE_PORT)));
            }
        }));
    }
}
