/*
 * Java 1.53.3 peer for the aeron-go adversarial mixed publisher/consumer matrix
 * (systests/adversarial/mixed-matrix). It is run as a killable docker container.
 *
 * Usage: MatrixPeer pub|sub key=value...
 *   pub: channel stream exclusive=true|false writer=N sizes=a,b,c rateUs=N dest=... (repeatable)
 *   sub: channel stream dest=... (repeatable)
 *
 * Wire format of message seq from writer w (identical to internal/mm/payload.go):
 *   length       = sizes[seq % sizes.length]
 *   payload[j]   = (byte)(seq * 31 + j + w * 7)
 *   reserved     = crc32(payload) << 32 | w << 24 | (seq & 0xFFFFFF)
 *
 * stdin "close" closes gracefully: pub emits {"event":"closed","writer":w,"next":seq,"firstPosition":p}
 * (p = publication position read just before seq 0 was offered), sub emits one "report"
 * line per image. One JSON object per stdout line.
 */
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.FragmentAssembler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

public final class MatrixPeer
{
    private static final Map<String, String> ARGS = new HashMap<>();
    private static final List<String> DESTS = new ArrayList<>();
    private static final AtomicBoolean CLOSE = new AtomicBoolean();
    private static final Map<Long, ImageState> IMAGES = new ConcurrentHashMap<>();

    public static void main(final String[] args)
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
        final Thread stdin = new Thread(() ->
        {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)))
            {
                String line;
                while ((line = r.readLine()) != null)
                {
                    if ("close".equals(line.trim()))
                    {
                        CLOSE.set(true);
                    }
                }
            }
            catch (final Exception ignore)
            {
            }
        });
        stdin.setDaemon(true);
        stdin.start();

        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(ARGS.getOrDefault("dir", "/dev/shm/aeron-adv-mixed-matrix"))
            .driverTimeoutMs(Long.parseLong(ARGS.getOrDefault("driverTimeoutMs", "3000")))
            .errorHandler(t -> emit("clientError", "message", String.valueOf(t)))
            .availableImageHandler(MatrixPeer::onAvailable)
            .unavailableImageHandler(MatrixPeer::onUnavailable);
        try (Aeron aeron = Aeron.connect(ctx))
        {
            if ("pub".equals(args[0]))
            {
                pub(aeron);
            }
            else
            {
                sub(aeron);
            }
        }
        catch (final Throwable t)
        {
            emit("terminated", "error", t.getClass().getSimpleName() + ": " + t.getMessage());
            System.exit(3);
        }
        System.exit(0);
    }

    static int[] sizes()
    {
        final String[] parts = ARGS.getOrDefault("sizes", "64").split(",");
        final int[] sizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++)
        {
            sizes[i] = Integer.parseInt(parts[i]);
        }
        return sizes;
    }

    static void fill(final UnsafeBuffer buffer, final long seq, final int writer, final int length)
    {
        for (int j = 0; j < length; j++)
        {
            buffer.putByte(j, (byte)(seq * 31 + j + writer * 7));
        }
    }

    static long crc(final DirectBuffer buffer, final int offset, final int length)
    {
        final CRC32 crc = new CRC32();
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        crc.update(bytes);
        return crc.getValue();
    }

    private static void pub(final Aeron aeron) throws Exception
    {
        final String channel = ARGS.get("channel");
        final int stream = Integer.parseInt(ARGS.get("stream"));
        final boolean exclusive = Boolean.parseBoolean(ARGS.getOrDefault("exclusive", "false"));
        final int writer = Integer.parseInt(ARGS.getOrDefault("writer", "1"));
        final long rateUs = Long.parseLong(ARGS.getOrDefault("rateUs", "200"));
        final int[] sizes = sizes();
        final Publication pub = exclusive ?
            aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream);
        for (final String d : DESTS)
        {
            pub.addDestination(d);
        }
        emit("ready", "sessionId", pub.sessionId(), "maxPayloadLength", pub.maxPayloadLength(),
            "maxMessageLength", pub.maxMessageLength());
        int max = 0;
        for (final int s : sizes)
        {
            max = Math.max(max, s);
        }
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Math.max(1, max)]);
        long seq = 0;
        long firstPosition = -1;
        while (!CLOSE.get())
        {
            final long before = pub.position();
            final int length = sizes[(int)(seq % sizes.length)];
            fill(buffer, seq, writer, length);
            final long rv = (crc(buffer, 0, length) << 32) | ((long)writer << 24) | (seq & 0xFFFFFF);
            final long result = pub.offer(buffer, 0, length, (tb, to, fl) -> rv);
            if (result > 0)
            {
                if (0 == seq)
                {
                    firstPosition = before;
                }
                seq++;
                if (rateUs > 0)
                {
                    LockSupport.parkNanos(rateUs * 1000);
                }
            }
            else if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
            {
                throw new IllegalStateException("offer: " + Publication.errorString(result));
            }
            else
            {
                Thread.onSpinWait();
            }
        }
        emit("closed", "writer", writer, "next", seq, "sessionId", pub.sessionId(), "firstPosition", firstPosition);
        pub.close();
    }

    static final class ImageState
    {
        final long correlationId;
        final int sessionId;
        final long joinPosition;
        final int termLength;
        long lastEnd = -1;
        long messages;
        long errors;
        String firstError = "";
        final Map<Integer, long[]> writers = new HashMap<>(); // writer -> {first, next}
        volatile boolean eos;

        ImageState(final Image image)
        {
            correlationId = image.correlationId();
            sessionId = image.sessionId();
            joinPosition = image.joinPosition();
            termLength = image.termBufferLength();
        }

        void error(final String e)
        {
            errors++;
            if (firstError.isEmpty())
            {
                firstError = e;
            }
        }

        void onFragment(final Header header)
        {
            final long end = header.position();
            final long start = end - BitUtil.align(header.frameLength(), 32);
            final long expected = lastEnd < 0 ? joinPosition : lastEnd;
            // forward jumps are padding: at term ends, or written by the driver's unblocker over the claim of a
            // crashed publisher; message continuity is still checked per writer in onMessage
            if (start < expected)
            {
                error("fragment at " + start + " expected " + expected);
            }
            lastEnd = end;
        }

        void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messages++;
            final long rv = header.reservedValue();
            final int writer = (int)((rv >>> 24) & 0xFF);
            final long seq = rv & 0xFFFFFF;
            final long crc = rv >>> 32;
            if (crc(buffer, offset, length) != crc)
            {
                error("crc mismatch writer " + writer + " seq " + seq);
                return;
            }
            for (int j = 0; j < length; j++)
            {
                if (buffer.getByte(offset + j) != (byte)(seq * 31 + j + writer * 7))
                {
                    error("payload mismatch writer " + writer + " seq " + seq + " at " + j);
                    return;
                }
            }
            final long[] w = writers.get(writer);
            if (null == w)
            {
                writers.put(writer, new long[]{ seq, (seq + 1) & 0xFFFFFF });
            }
            else if (w[1] != seq)
            {
                error("writer " + writer + " expected seq " + w[1] + " got " + seq);
                w[1] = (seq + 1) & 0xFFFFFF;
            }
            else
            {
                w[1] = (seq + 1) & 0xFFFFFF;
            }
        }

        String writerSummary()
        {
            final StringBuilder sb = new StringBuilder();
            for (final Map.Entry<Integer, long[]> e : writers.entrySet())
            {
                sb.append(e.getKey()).append(':').append(e.getValue()[0]).append(':').append(e.getValue()[1])
                    .append(';');
            }
            return sb.toString();
        }
    }

    private static void onAvailable(final Image image)
    {
        IMAGES.putIfAbsent(image.correlationId(), new ImageState(image)); // poll may see the image first
        emit("available", "sessionId", image.sessionId(), "correlationId", image.correlationId(),
            "joinPosition", image.joinPosition());
    }

    private static void onUnavailable(final Image image)
    {
        final ImageState s = IMAGES.get(image.correlationId());
        if (null != s)
        {
            s.eos = image.isEndOfStream();
        }
        emit("unavailable", "sessionId", image.sessionId(), "correlationId", image.correlationId(),
            "eos", image.isEndOfStream());
    }

    private static void sub(final Aeron aeron)
    {
        final Subscription sub = aeron.addSubscription(ARGS.get("channel"), Integer.parseInt(ARGS.get("stream")));
        for (final String d : DESTS)
        {
            sub.addDestination(d);
        }
        emit("ready", "registrationId", sub.registrationId());
        final ImageState[] current = new ImageState[1];
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) ->
        {
            if (null != current[0])
            {
                current[0].onMessage(buffer, offset, length, header);
            }
        });
        final FragmentHandler raw = (buffer, offset, length, header) ->
        {
            final Image image = (Image)header.context();
            current[0] = IMAGES.computeIfAbsent(image.correlationId(), id -> new ImageState(image));
            current[0].onFragment(header);
            assembler.onFragment(buffer, offset, length, header);
        };
        while (!CLOSE.get())
        {
            if (sub.poll(raw, 64) == 0)
            {
                if (sub.isClosed())
                {
                    throw new IllegalStateException("subscription closed");
                }
                LockSupport.parkNanos(50_000);
            }
        }
        for (final ImageState s : IMAGES.values())
        {
            emit("report", "sessionId", s.sessionId, "correlationId", s.correlationId, "messages", s.messages,
                "errors", s.errors, "firstError", s.firstError, "writers", s.writerSummary(), "eos", s.eos);
        }
        emit("done");
        sub.close();
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
                sb.append('"').append(String.valueOf(v).replace("\\", "\\\\").replace("\"", "'")).append('"');
            }
        }
        System.out.println(sb.append('}'));
        System.out.flush();
    }
}
