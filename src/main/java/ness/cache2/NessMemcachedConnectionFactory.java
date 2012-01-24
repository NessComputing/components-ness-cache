package ness.cache2;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.transcoders.Transcoder;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;

import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.likeness.logging.Log;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends BinaryConnectionFactory
{
    private static final Log LOG = Log.findLog();

    private final CacheConfiguration configuration;
    private final NessKetamaNodeLocator nodeLocator;
    private final String jmxPrefix;

    private MBeanExporter exporter;

    @Inject
    NessMemcachedConnectionFactory(final CacheConfiguration configuration,
                                   @Nullable @Named("cacheName") final String cacheName)
    {
        super(configuration.getIncomingQueueSize(),
              configuration.getReadBufferSize(),
              DefaultHashAlgorithm.KETAMA_HASH);

        this.configuration = configuration;
        this.nodeLocator = new NessKetamaNodeLocator(configuration);
        this.jmxPrefix = "ness.memcached:cache=" + Objects.firstNonNull(cacheName, "[default]");

    }

    @Inject(optional=true)
    void injectExporter(final MBeanExporter exporter)
    {
        this.exporter = exporter;
    }

    // Use our custom transcoder

    @SuppressWarnings(value = {"unchecked", "rawtypes"} )
    @Override
    public Transcoder<Object> getDefaultTranscoder()
    {
        return (Transcoder) new MemcacheByteArrayTranscoder();
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

    @Override
    public final NodeLocator createLocator(final List<MemcachedNode> nodes)
    {
        this.nodeLocator.addNodes(nodes);
        return this.nodeLocator;
    }

    @Override
    public final MemcachedNode createMemcachedNode(final SocketAddress sa, final int bufSize) throws IOException
    {
        final MemcachedNode node = super.createMemcachedNode(sa, bufSize);
        if (exporter != null) {
            final StringBuilder sb = new StringBuilder(jmxPrefix);
            sb.append(",node=");
            jmxAppend(node.getSocketAddress().toString(), sb);
            try {
                exporter.export(sb.toString(), node);
            }
            catch (JmxException je) {
                LOG.warnDebug(je, "Could not export Memcached node %s to JMX!", sb);
            }
        }
        return node;
    }

    public NessKetamaNodeLocator getNodeLocator()
    {
        return this.nodeLocator;
    }

    @Override
    public final void destroyMemcachedNode(final MemcachedNode n)
    {
        if (n != null) {
            if (exporter != null) {
                final StringBuilder sb = new StringBuilder(jmxPrefix);
                sb.append(",node=");
                jmxAppend(n.getSocketAddress().toString(), sb);
                try {
                    exporter.unexport(sb.toString());
                }
                catch (JmxException je) {
                    LOG.warnDebug(je, "Could not unexport Memcached node %s from JMX!", sb);
                }
            }
            super.destroyMemcachedNode(n);
        }
    }

    public final MemcachedNode createMemcachedNode(final SocketAddress sa) throws IOException
    {
        return this.createMemcachedNode(sa, configuration.getReadBufferSize());
    }

    private final void jmxAppend(final String input, final StringBuilder sb)
    {
        if (input == null) {
            return;
        }

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
    }
}
