package ness.cache;

import io.trumpet.log.Log;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import ness.discovery.client.ReadOnlyDiscoveryClient;
import ness.discovery.client.ServiceInformation;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Use service discovery or configuration to assemble a set of Memcache servers dynamically.
 */
@Singleton
class CacheTopologyProvider {
    private static final Log LOG = Log.findLog();
    private final Set<InetSocketAddress> addrs;
    private final ReadOnlyDiscoveryClient discoveryClient;
    private String cacheName;

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
    
    @Inject(optional=true)
    public void setCacheName(@Named("cacheName") String cacheName) {
    	this.cacheName = cacheName;
    }

    public Set<InetSocketAddress> get() {
        if (addrs != null) {
            return addrs;
        }

        Collection<ServiceInformation> serviceInformation;
        if (cacheName == null) {
        	serviceInformation = discoveryClient.findAllServiceInformation("memcached");
        } else {
        	serviceInformation = discoveryClient.findAllServiceInformation("memcached", cacheName);
        	//Discovery might have fallen back to un-typed entries, and we want strict typing
        	serviceInformation = Collections2.filter(serviceInformation, new Predicate<ServiceInformation>() {
				@Override
				public boolean apply(ServiceInformation input) {
					return cacheName.equals(input.getServiceType());
				}
			});
        }
		return ImmutableSet.copyOf(Collections2.transform(serviceInformation, new Function<ServiceInformation, InetSocketAddress>() {
            @Override
            public InetSocketAddress apply(ServiceInformation input) {
                return new InetSocketAddress(input.getProperty(ServiceInformation.PROP_SERVICE_ADDRESS), 
                        Integer.valueOf(input.getProperty(ServiceInformation.PROP_SERVICE_PORT)));
            }
        }));
    }
}
