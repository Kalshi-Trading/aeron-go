/*
 * Java 1.53.3 peer for the aeron-go adversarial publisher-crash suite (systests/adversarial/publisher-crash).
 *
 *   CrashTool pub dir=<aeron dir> channel=<uri> stream=<id> [exclusive=true] [mode=offer|claim|holdclaim]
 *                 [size=<bytes>] [count=<n, -1 forever>] [from=<seq>] [intervalUs=<us>] [holdAfter=<n>]
 *                 [holdFragment=<k>] [dest=<uri>]...
 *   CrashTool sub dir=<aeron dir> channel=<uri> stream=<id> [dest=<uri>]...
 *
 * Messages use the Go e2e format (e2e.EncodeMessage): bytes [0,8) little-endian sequence, [8,12) little-endian
 * length, byte i >= 12 is (byte)(seq ^ i). Events are JSON lines on stdout, the same as the Go crashclient helper.
 * mode=holdclaim/holdfrag behave like the Go crashclient: a never committed claim, or a fragmented message blocked
 * in the ReservedValueSupplier of fragment holdFragment.
 * stdin commands: "report" (sub: per-session stats), "exit".
 */
package io.aeron.adversarial;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

public final class CrashTool
{
    private static final Map<String, String> ARGS = new HashMap<>();
    private static final List<String> DESTS = new ArrayList<>();
    private static final ConcurrentLinkedQueue<String> STDIN = new ConcurrentLinkedQueue<>();

    public static void main(final String[] args) throws Exception
    {
        for (int i = 1; i < args.length; i++)
        {
            final int idx = args[i].indexOf('=');
            final String k = args[i].substring(0, idx);
            final String v = args[i].substring(idx + 1);
            if ("dest".equals(k))
            {
                DESTS.add(v);
            }
            else
            {
                ARGS.put(k, v);
            }
        }
        startStdinReader();
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(str("dir", "/dev/shm/aeron-adv-publisher-crash"))
            .errorHandler((t) -> emit("error", "type", t.getClass().getSimpleName(), "message", String.valueOf(t)))
            .availableImageHandler((image) -> emit("image", "sessionId", image.sessionId(),
                "joinPosition", image.joinPosition(), "correlationId", image.correlationId()))
            .unavailableImageHandler((image) -> emit("imageGone", "sessionId", image.sessionId(),
                "position", image.position(), "correlationId", image.correlationId()));
        try (Aeron aeron = Aeron.connect(ctx))
        {
            switch (args[0])
            {
                case "pub": pub(aeron); break;
                case "sub": sub(aeron); break;
                default: throw new IllegalArgumentException("unknown mode " + args[0]);
            }
        }
        catch (final Throwable t)
        {
            emit("fatal", "type", t.getClass().getSimpleName(), "message", String.valueOf(t));
            System.exit(1);
        }
        System.exit(0);
    }

    private static void pub(final Aeron aeron) throws Exception
    {
        final String channel = str("channel", null);
        final int stream = Integer.parseInt(str("stream", null));
        final boolean exclusive = Boolean.parseBoolean(str("exclusive", "false"));
        final String mode = str("mode", "offer");
        final int size = Integer.parseInt(str("size", "64"));
        final long count = Long.parseLong(str("count", "-1"));
        final long from = Long.parseLong(str("from", "0"));
        final long intervalUs = Long.parseLong(str("intervalUs", "0"));
        final long holdAfter = Long.parseLong(str("holdAfter", "-1"));
        final long holdFragment = Long.parseLong(str("holdFragment", "1"));

        final Publication pub = exclusive ?
            aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream);
        for (final String d : DESTS)
        {
            pub.addDestination(d);
        }
        emit("ready", "sessionId", pub.sessionId(), "registrationId", pub.registrationId(), "clientId", aeron.clientId(),
            "initialTermId", pub.initialTermId(), "termLength", pub.termBufferLength(), "position", pub.position(),
            "maxPayloadLength", pub.maxPayloadLength());
        final long deadline = System.currentTimeMillis() + 30_000;
        while (!pub.isConnected())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("publication not connected");
            }
            Thread.sleep(1);
        }
        emit("connected", "sessionId", pub.sessionId(), "position", pub.position());

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Math.max(size, 12)]);
        final BufferClaim claim = new BufferClaim();
        long lastProgressMs = 0;
        long sent = 0;
        for (long seq = from; count < 0 || seq < from + count; seq++)
        {
            if (sent == holdAfter && "holdclaim".equals(mode))
            {
                long result;
                while ((result = pub.tryClaim(size, claim)) <= 0)
                {
                    checkResult(result);
                    Thread.onSpinWait();
                }
                encode(claim.buffer(), claim.offset(), seq, size);
                emit("holding", "seq", seq, "position", result);
                while (true)
                {
                    Thread.sleep(1000);
                }
            }
            while (true)
            {
                final long result;
                if ("offer".equals(mode) || "holdclaim".equals(mode))
                {
                    encode(buffer, 0, seq, size);
                    result = pub.offer(buffer, 0, size);
                }
                else if ("holdfrag".equals(mode))
                {
                    encode(buffer, 0, seq, size);
                    final long holdSeq = seq;
                    final long[] fragment = {0};
                    result = sent != holdAfter ? pub.offer(buffer, 0, size) :
                        pub.offer(buffer, 0, size, (termBuffer, termOffset, frameLength) ->
                        {
                            if (fragment[0]++ == holdFragment)
                            {
                                emit("holding", "seq", holdSeq, "termOffset", termOffset, "position", pub.position());
                                LockSupport.park();
                                while (true)
                                {
                                    Thread.onSpinWait();
                                }
                            }
                            return 0;
                        });
                }
                else
                {
                    result = pub.tryClaim(size, claim);
                    if (result > 0)
                    {
                        encode(claim.buffer(), claim.offset(), seq, size);
                        claim.commit();
                    }
                }
                if (result > 0)
                {
                    break;
                }
                checkResult(result);
                Thread.onSpinWait();
            }
            sent++;
            final long nowMs = System.currentTimeMillis();
            if (nowMs - lastProgressMs >= 20)
            {
                lastProgressMs = nowMs;
                emit("progress", "next", seq + 1, "position", pub.position());
            }
            if (intervalUs > 0)
            {
                LockSupport.parkNanos(intervalUs * 1000);
            }
            if ("exit".equals(STDIN.peek()))
            {
                break;
            }
        }
        emit("done", "next", from + sent, "position", pub.position());
        while (!"exit".equals(STDIN.poll()))
        {
            Thread.sleep(10);
        }
        pub.close();
    }

    private static void checkResult(final long result)
    {
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("offer failed: " + result);
        }
    }

    private static void sub(final Aeron aeron) throws Exception
    {
        final Subscription sub = aeron.addSubscription(str("channel", null), Integer.parseInt(str("stream", null)));
        for (final String d : DESTS)
        {
            sub.addDestination(d);
        }
        emit("ready", "registrationId", sub.registrationId(), "clientId", aeron.clientId());
        final Map<Integer, Stats> stats = new TreeMap<>();
        final FragmentHandler handler = new FragmentAssembler((buffer, offset, length, header) ->
            stats.computeIfAbsent(header.sessionId(), (k) -> new Stats()).onMessage(buffer, offset, length, header));
        long total = 0;
        long lastProgressMs = 0;
        while (true)
        {
            final int n = sub.poll(handler, 64);
            if (n == 0)
            {
                LockSupport.parkNanos(100_000);
            }
            final String cmd = STDIN.poll();
            if ("report".equals(cmd))
            {
                for (final Map.Entry<Integer, Stats> e : stats.entrySet())
                {
                    final Stats s = e.getValue();
                    emit("session", "sessionId", e.getKey(), "first", s.first, "next", s.next,
                        "received", s.received, "gaps", s.gaps, "duplicates", s.duplicates, "corrupt", s.corrupt,
                        "lastPosition", s.lastPosition);
                }
                emit("reportEnd", "images", sub.imageCount());
            }
            else if ("exit".equals(cmd) || "EOF".equals(cmd))
            {
                break;
            }
            final long nowMs = System.currentTimeMillis();
            if (nowMs - lastProgressMs >= 100)
            {
                lastProgressMs = nowMs;
                long received = 0;
                for (final Stats s : stats.values())
                {
                    received += s.received;
                }
                if (received != total)
                {
                    total = received;
                    emit("progress", "received", total);
                }
            }
        }
        sub.close();
    }

    static final class Stats
    {
        long first = -1;
        long next = -1;
        long received;
        long gaps;
        long duplicates;
        long corrupt;
        long lastPosition;

        void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            received++;
            lastPosition = header.position();
            if (length < 12 || buffer.getInt(offset + 8, ByteOrder.LITTLE_ENDIAN) != length)
            {
                corrupt++;
                return;
            }
            final long seq = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
            for (int i = 12; i < length; i++)
            {
                if (buffer.getByte(offset + i) != (byte)(seq ^ i))
                {
                    corrupt++;
                    return;
                }
            }
            if (first < 0)
            {
                first = seq;
                next = seq + 1;
            }
            else if (seq == next)
            {
                next++;
            }
            else if (seq > next)
            {
                gaps++;
                next = seq + 1;
            }
            else
            {
                duplicates++;
            }
        }
    }

    static void encode(final MutableDirectBuffer buffer, final int offset, final long seq, final int length)
    {
        buffer.putLong(offset, seq, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(offset + 8, length, ByteOrder.LITTLE_ENDIAN);
        for (int i = 12; i < length; i++)
        {
            buffer.putByte(offset + i, (byte)(seq ^ i));
        }
    }

    private static void startStdinReader()
    {
        final Thread t = new Thread(() ->
        {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)))
            {
                String line;
                while ((line = r.readLine()) != null)
                {
                    STDIN.add(line.trim());
                }
            }
            catch (final Exception ignore)
            {
            }
        });
        t.setDaemon(true);
        t.start();
    }

    static String str(final String key, final String dflt)
    {
        final String v = ARGS.getOrDefault(key, dflt);
        if (v == null)
        {
            throw new IllegalArgumentException("missing " + key + "=");
        }
        return v;
    }

    static synchronized void emit(final String event, final Object... kv)
    {
        final StringBuilder sb = new StringBuilder("{\"event\":\"").append(event).append('"');
        for (int i = 0; i < kv.length; i += 2)
        {
            sb.append(",\"").append(kv[i]).append("\":");
            final Object v = kv[i + 1];
            if (v instanceof Number || v instanceof Boolean)
            {
                sb.append(v);
            }
            else
            {
                sb.append('"').append(String.valueOf(v).replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            }
        }
        System.out.println(sb.append('}'));
        System.out.flush();
    }
}
