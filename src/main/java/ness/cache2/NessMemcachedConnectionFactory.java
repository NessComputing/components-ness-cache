package ness.cache2;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedNodeStats;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.likeness.logging.Log;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {

    private static final Log LOG = Log.findLog();

    private final CacheConfiguration configuration;
    private final String cacheName;
    private MBeanExporter exporter = null;
    private final AtomicInteger nodeGeneration = new AtomicInteger();
    private final ConcurrentMap<SocketAddress, MemcachedNodeStats> statsMap = Maps.newConcurrentMap();

    @Inject
    NessMemcachedConnectionFactory(final CacheConfiguration configuration,
                                   @Nullable @Named("cacheName") final String cacheName) {
        this.configuration = configuration;
        this.cacheName = cacheName == null ? "[default]" : cacheName;
    }

    @Inject(optional=true)
    void injectMBeanExporter(final MBeanExporter exporter)
    {
        this.exporter = exporter;
    }

    // Use our custom transcoder

    @SuppressWarnings(value = {"unchecked", "rawtypes"} )
    @Override
    public Transcoder<Object> getDefaultTranscoder() {
        return (Transcoder) new MemcacheByteArrayTranscoder();
    }

    @Override
    public MemcachedNode createMemcachedNode(SocketAddress sa, int bufSize) throws IOException
    {
        boolean doAuth = false;

        final String jmxName = String.format("ness.memcached:cache=%s,node=%s-%d", this.cacheName, jmxSafe(sa.toString()), nodeGeneration.getAndIncrement());

        final MemcachedNode node = new NessMemcachedNode(sa, bufSize,
                                                         createReadOperationQueue(),
                                                         createWriteOperationQueue(),
                                                         createOperationQueue(),
                                                         configuration.getOperationQueueBlockTime().getMillis(),
                                                         doAuth,
                                                         getOperationTimeout(),
                                                         getMemcachedNodeStats(sa),
                                                         jmxName);

        if (exporter != null) {
            try {
                exporter.export(jmxName, node);
            }
            catch (JmxException je) {
                LOG.warnDebug(je, "Could not export new memcached node for %s", sa);
            }
        }

        return node;
    }

    @Override
    public void destroyMemcachedNode(final MemcachedNode node)
    {
        if (exporter != null && node instanceof NessMemcachedNode) {
            try {
                exporter.unexport(NessMemcachedNode.class.cast(node).getJmxName());
            }
            catch (JmxException je) {
                LOG.warnDebug(je, "Could not unexport existing memcached node for %s", node.getSocketAddress());
            }
        }
    }

    @Override
    public OperationFactory getOperationFactory()
    {
        return new BinaryOperationFactory();
    }

    @Override
    public final int getOpQueueLen()
    {
        return configuration.getIncomingQueueSize();
    }

    @Override
    public final BlockingQueue<Operation> createReadOperationQueue()
    {
        return new LinkedBlockingQueue<Operation>(configuration.getReadQueueSize());
    }

    @Override
    public final BlockingQueue<Operation> createWriteOperationQueue()
    {
        return new LinkedBlockingQueue<Operation>(configuration.getWriteQueueSize());
    }

    @Override
    public final long getOpQueueMaxBlockTime()
    {
        return configuration.getOperationQueueBlockTime().getMillis();
    }

    @Override
    public final long getOperationTimeout()
    {
        return configuration.getOperationTimeout().getMillis();
    }

    @Override
    public boolean isDaemon()
    {
        return configuration.isDaemonThreads();
    }

    public MemcachedNodeStats getMemcachedNodeStats(final SocketAddress sa)
    {
      final MemcachedNodeStats stats = new MemcachedNodeStats();
      final MemcachedNodeStats oldStats = statsMap.putIfAbsent(sa, stats);
      return (oldStats == null) ? stats : oldStats;
    }

    private String jmxSafe(String input) {
        if (input == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            switch(c){
                case ',':
                case '=':
                case ':':
                case '*':
                case '?':
                   sb.append("_");
                   break;
                default:
                   sb.append(c);
            }
        }
        return sb.toString();
    }

    public static class NessMemcachedNode extends BinaryMemcachedNodeImpl
    {
        public NessMemcachedNode(final SocketAddress sa,
                                 final int bufSize,
                                 final BlockingQueue<Operation> rq,
                                 final BlockingQueue<Operation> wq,
                                 final BlockingQueue<Operation> iq,
                                 final long opQueueMaxBlockTime,
                                 final boolean waitForAuth,
                                 final long dt,
                                 final MemcachedNodeStats stats,
                                 final String jmxName) throws IOException {
            super (sa, bufSize, rq, wq, iq, opQueueMaxBlockTime, waitForAuth, dt, stats);
            this.jmxName = jmxName;
        }

        private final String jmxName;

        public String getJmxName()
        {
            return jmxName;
        }
    }


}
