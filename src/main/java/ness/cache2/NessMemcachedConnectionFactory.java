package ness.cache2;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import net.spy.memcached.FailureMode;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nesscomputing.logging.Log;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {

    private static final Log LOG = Log.findLog();

    private final CacheConfiguration configuration;
    private final String cacheName;
    private MBeanExporter exporter = null;
    private final AtomicInteger nodeGeneration = new AtomicInteger();

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
    public MemcachedNode createMemcachedNode(SocketAddress sa, SocketChannel ch, int bufSize)
    {
        boolean doAuth = false;

        final String jmxName = String.format("ness.memcached:cache=%s,node=%s-%d", this.cacheName, jmxSafe(sa.toString()), nodeGeneration.getAndIncrement());

        final MemcachedNode node = new NessMemcachedNode(sa, ch, bufSize,
                                                         createReadOperationQueue(),
                                                         createWriteOperationQueue(),
                                                         createOperationQueue(),
                                                         configuration.getMemcachedOperationQueueBlockTime().getMillis(),
                                                         doAuth,
                                                         getOperationTimeout(),
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
    public OperationFactory getOperationFactory()
    {
        return new BinaryOperationFactory();
    }

    @Override
    public final int getOpQueueLen()
    {
        return configuration.getMemcachedIncomingQueueSize();
    }

    @Override
    public final BlockingQueue<Operation> createReadOperationQueue()
    {
        int queueSize = configuration.getMemcachedReadQueueSize();
        if (queueSize <= 0) {
            return new LinkedBlockingQueue<Operation>();
        }
        else {
            return new LinkedBlockingQueue<Operation>(queueSize);
        }
    }

    @Override
    public final BlockingQueue<Operation> createWriteOperationQueue()
    {
        int queueSize = configuration.getMemcachedWriteQueueSize();
        if (queueSize <= 0) {
            return new LinkedBlockingQueue<Operation>();
        }
        else {
            return new LinkedBlockingQueue<Operation>(queueSize);
        }
    }

    @Override
    public final long getOpQueueMaxBlockTime()
    {
        return configuration.getMemcachedOperationQueueBlockTime().getMillis();
    }

    @Override
    public final long getOperationTimeout()
    {
        return configuration.getMemcachedOperationTimeout().getMillis();
    }

    @Override
    public boolean isDaemon()
    {
        return configuration.isMemcachedDaemonThreads();
    }

    @Override
    public FailureMode getFailureMode()
    {
        return configuration.getMemcachedFailureMode();
    }

    @Override
    protected String getName()
    {
        return "NessMemcachedConnectionFactory";
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
                                 final SocketChannel ch,
                                 final int bufSize,
                                 final BlockingQueue<Operation> rq,
                                 final BlockingQueue<Operation> wq,
                                 final BlockingQueue<Operation> iq,
                                 final long opQueueMaxBlockTime,
                                 final boolean waitForAuth,
                                 final long dt,
                                 final String jmxName) {
            super (sa, ch, bufSize, rq, wq, iq, opQueueMaxBlockTime, waitForAuth, dt);
            this.jmxName = jmxName;
        }

        private final String jmxName;

        public String getJmxName()
        {
            return jmxName;
        }
    }


}
