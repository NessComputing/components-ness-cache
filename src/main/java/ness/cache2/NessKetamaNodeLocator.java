package ness.cache2;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.KetamaIterator;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedNodeROImpl;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.compat.SpyObject;

import com.likeness.logging.Log;

public final class NessKetamaNodeLocator extends SpyObject implements NodeLocator
{
    private static final Log LOG = Log.findLog();

    private final AtomicReference<KetamaState> stateHolder = new AtomicReference<KetamaState>();
    private final CacheConfiguration cacheConfiguration;

    public NessKetamaNodeLocator(final CacheConfiguration cacheConfiguration)
    {
        this.cacheConfiguration = cacheConfiguration;
        this.stateHolder.set(new KetamaState(Collections.<MemcachedNode>emptyList()));
    }

    private NessKetamaNodeLocator(final CacheConfiguration cacheConfiguration, final KetamaState state)
    {
        this.cacheConfiguration = cacheConfiguration;
        this.stateHolder.set(state);
    }

    @Override
    public MemcachedNode getPrimary(final String k)
    {
        final SortedMap<Long, MemcachedNode> ketamaMap = stateHolder.get().getKetamaMap();
        if (ketamaMap.isEmpty()) {
            return null;
        }

        long hashValue = DefaultHashAlgorithm.KETAMA_HASH.hash(k);
        if (!ketamaMap.containsKey(hashValue)) {
            // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
            // in a lot of places, so I'm doing this myself.
            final SortedMap<Long, MemcachedNode> tailMap = ketamaMap.tailMap(hashValue);
            if (tailMap.isEmpty()) {
                hashValue = ketamaMap.firstKey();
            } else {
                hashValue = tailMap.firstKey();
            }
        }
        final MemcachedNode node = ketamaMap.get(hashValue);
        if (node == null) {
            return null;
        }
        return node;
    }

    @Override
    public Iterator<MemcachedNode> getSequence(final String k)
    {
        // Seven searches gives us a 1 in 2^7 chance of hitting the
        // same dead node all of the time.
        final SortedMap<Long, MemcachedNode> ketamaMap = stateHolder.get().getKetamaMap();
        return new KetamaIterator(k, 7, ketamaMap, DefaultHashAlgorithm.KETAMA_HASH);
    }

    @Override
    public Collection<MemcachedNode> getAll()
    {
        return stateHolder.get().getNodeList();
    }

    @Override
    public NodeLocator getReadonlyCopy()
    {
        final Collection<MemcachedNode> nodes = getAll();
        final List<MemcachedNode> roNodes = new ArrayList<MemcachedNode>(nodes.size());
        for (MemcachedNode n : nodes) {
            roNodes.add(new MemcachedNodeROImpl(n));
        }

        return new NessKetamaNodeLocator(cacheConfiguration, new KetamaState(roNodes));
    }

    @Override
    public void updateLocator(final List<MemcachedNode> nodes)
    {
        KetamaState currentState = null;
        KetamaState newState = null;
        do {
            currentState = stateHolder.get();
            newState = new KetamaState(nodes);
        } while (!stateHolder.compareAndSet(currentState, newState));
    }

    public void addNodes(final Collection<MemcachedNode> nodes) {
        KetamaState currentState = null;
        KetamaState newState = null;
        do {
            currentState = stateHolder.get();
            final Map<String, MemcachedNode> nodeKeys = currentState.getNodeKeys();
            final List<MemcachedNode> newNodes = new ArrayList<MemcachedNode>(currentState.getNodeList());

            for (final MemcachedNode newNode : nodes) {
                final String nodeKey = getKey(newNode.getSocketAddress());
                if (nodeKeys.get(nodeKey) == null) {
                    newNodes.add(newNode);
                    LOG.debug("Adding new Node %s to cache ring", nodeKey);
                }
                else {
                    LOG.warn("Ignoring Node %s, already exists in cache ring!", nodeKey);
                }
            }
            newState = new KetamaState(newNodes);
        } while (!stateHolder.compareAndSet(currentState, newState));
    }

    public void removeNodes(final Collection<MemcachedNode> nodes) {
        KetamaState currentState = null;
        KetamaState newState = null;
        do {
            currentState = stateHolder.get();
            final Map<String, MemcachedNode> nodeKeys = currentState.getNodeKeys();
            final List<MemcachedNode> newNodes = new ArrayList<MemcachedNode>(currentState.getNodeList());

            for (final MemcachedNode newNode : nodes) {
                final String nodeKey = getKey(newNode.getSocketAddress());
                if (nodeKeys.get(nodeKey) == null) {
                    LOG.warn("Can not remove Node %s, not in cache ring!", nodeKey);
                }
                else {
                    LOG.debug("Removing Node %s,from cache ring!", nodeKey);
                }
            }
            newState = new KetamaState(newNodes);
        } while (!stateHolder.compareAndSet(currentState, newState));
    }

    public Map<String, MemcachedNode> getNodeKeys()
    {
        return stateHolder.get().getNodeKeys();
    }

    public static String getKey(final SocketAddress sa)
    {
        String keyPrefix = String.valueOf(sa);
        if (keyPrefix.startsWith("/")) {
            keyPrefix = keyPrefix.substring(1);
        }
        return keyPrefix;
    }

    private class KetamaState
    {
        private final List<MemcachedNode> nodes;
        private final SortedMap<Long, MemcachedNode> ketamaMap = new TreeMap<Long, MemcachedNode>();
        private final Map<String, MemcachedNode> nodeKeys = new HashMap<String, MemcachedNode>();

        KetamaState(final List<MemcachedNode> nodes)
        {
            // Make sure that we do not keep a reference to the list object.
            this.nodes = new ArrayList<MemcachedNode>(nodes);
            buildKetamaMap(this.nodes);
        }

        public List<MemcachedNode> getNodeList()
        {
            return nodes;
        }

        public SortedMap<Long, MemcachedNode> getKetamaMap()
        {
            return ketamaMap;
        }

        public Map<String, MemcachedNode> getNodeKeys()
        {
            return nodeKeys;
        }

        private void buildKetamaMap(final List<MemcachedNode> nodes)
        {
            // Copied from KetamaNodeLocator#setKetamaNodes
            final int numReps = cacheConfiguration.getNodeRepetitions();

            for (final MemcachedNode node : nodes) {

                String keyPrefix = getKey(node.getSocketAddress());
                final MemcachedNode oldNode = nodeKeys.put(keyPrefix, node);
                if (oldNode != null) {
                    LOG.warn("Added Node %s for key '%s', but another node (%s) already existed!", node, keyPrefix, oldNode);
                }
                keyPrefix = keyPrefix + "-";

                // Ketama does some special work with md5 where it reuses chunks.
                for (int i = 0; i < numReps / 4; i++) {
                    final String key = keyPrefix + i;
                    final byte[] digest = DefaultHashAlgorithm.computeMd5(key);
                    for (int h = 0; h < 4; h++) {
                        final Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                            | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                            | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                            | (digest[h * 4] & 0xFF);
                        ketamaMap.put(k, node);
                        LOG.trace("Adding node %s in position %d", node, k);
                    }
                }
            }
            assert ketamaMap.size() == numReps * nodes.size();
        }
    }
}
