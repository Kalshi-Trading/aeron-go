/*
 * Java 1.53.3 counterpart of systests/adversarial/driver-crash/internal/crashpeer: one Aeron client that publishes
 * and/or subscribes and reports, as JSON lines on stdout, every error-handler call, close-handler call and image
 * event, so the Go tests can compare the Java client's reaction to a driver crash with the Go client's.
 *
 * Args (key=value): dir, channel, stream, role=pub|sub|pubsub|idle, exclusive=true|false, claim=true|false,
 * size, intervalUs, driverTimeoutMs, lingerMs (how long to stay alive after the client terminated).
 */
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.exceptions.ClientTimeoutException;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class JavaPeer
{
    private static final Map<String, String> ARGS = new HashMap<>();

    public static void main(final String[] args) throws Exception
    {
        for (final String a : args)
        {
            final int idx = a.indexOf('=');
            ARGS.put(a.substring(0, idx), a.substring(idx + 1));
        }
        final String role = str("role", "pubsub");
        final int size = Integer.parseInt(str("size", "64"));
        final long intervalNs = Long.parseLong(str("intervalUs", "1000")) * 1000;

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(str("dir", "/dev/shm/aeron-adv-driver-crash"))
            .driverTimeoutMs(Long.parseLong(str("driverTimeoutMs", "2000")))
            .errorHandler((t) -> emit("error", "\"type\":\"" + classify(t) + "\",\"message\":" + quote(t.toString())))
            .availableImageHandler((img) -> emit("available", "\"sessionId\":" + img.sessionId()))
            .unavailableImageHandler((img) -> emit("unavailable", "\"sessionId\":" + img.sessionId()));
        final Aeron aeron = Aeron.connect(ctx);
        aeron.addCloseHandler(() -> emit("closed", null));

        final String channel = str("channel", "aeron:ipc");
        final int stream = Integer.parseInt(str("stream", "9000"));
        Publication pub = null;
        Subscription sub = null;
        if (role.contains("sub"))
        {
            sub = aeron.addSubscription(channel, stream);
        }
        if (role.contains("pub"))
        {
            pub = Boolean.parseBoolean(str("exclusive", "false")) ?
                aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream);
        }
        emit("ready", "\"clientId\":" + aeron.clientId() + ",\"sessionId\":" + (null == pub ? 0 : pub.sessionId()));

        final boolean claim = Boolean.parseBoolean(str("claim", "false"));
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Math.max(size, 12)]);
        final BufferClaim bufferClaim = new BufferClaim();
        final AtomicLong received = new AtomicLong();
        final FragmentHandler handler = (b, o, l, h) -> received.incrementAndGet();
        long seq = 0;
        long nextProgressNs = System.nanoTime();
        String lastOffer = "";
        try
        {
            while (!aeron.isClosed())
            {
                if (null != pub)
                {
                    final long result;
                    if (claim)
                    {
                        result = pub.tryClaim(size, bufferClaim);
                        if (result > 0)
                        {
                            bufferClaim.buffer().putLong(bufferClaim.offset(), seq);
                            bufferClaim.commit();
                        }
                    }
                    else
                    {
                        buffer.putLong(0, seq);
                        result = pub.offer(buffer, 0, size);
                    }
                    if (result > 0)
                    {
                        seq++;
                    }
                    else if (result == Publication.CLOSED)
                    {
                        lastOffer = "CLOSED";
                    }
                }
                if (null != sub)
                {
                    sub.poll(handler, 64);
                }
                if (System.nanoTime() - nextProgressNs > 0)
                {
                    emit("progress", "\"sent\":" + seq + ",\"received\":" + received.get() +
                        ",\"lastOffer\":\"" + lastOffer + "\"");
                    nextProgressNs = System.nanoTime() + 200_000_000L;
                }
                LockSupport.parkNanos(intervalNs);
            }
        }
        catch (final Throwable t)
        {
            emit("exception", "\"type\":\"" + classify(t) + "\",\"message\":" + quote(t.toString()));
        }
        final String offerAfter = null == pub ? "" : String.valueOf(pub.offer(buffer, 0, size));
        emit("terminated", "\"offerAfter\":\"" + offerAfter + "\",\"sent\":" + seq);
        final long lingerMs = Long.parseLong(str("lingerMs", "500"));
        Thread.sleep(lingerMs);
        aeron.close();
        emit("exit", null);
        System.exit(0);
    }

    private static String classify(final Throwable t)
    {
        if (t instanceof DriverTimeoutException)
        {
            return "DriverTimeout";
        }
        if (t instanceof ClientTimeoutException)
        {
            return "ClientTimeout";
        }
        if (t instanceof ConductorServiceTimeoutException)
        {
            return "ConductorServiceTimeout";
        }
        return t.getClass().getSimpleName();
    }

    private static String str(final String k, final String dflt)
    {
        return ARGS.getOrDefault(k, dflt);
    }

    private static String quote(final String s)
    {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + "\"";
    }

    private static synchronized void emit(final String event, final String fields)
    {
        System.out.println("{\"event\":\"" + event + "\",\"lang\":\"java\",\"tMs\":" + System.currentTimeMillis() +
            (null == fields ? "" : "," + fields) + "}");
        System.out.flush();
    }
}
