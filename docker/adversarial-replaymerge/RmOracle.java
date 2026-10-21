/*
 * Java Aeron 1.53.3 oracle for systests/adversarial/replaymerge. Runs a ReplayMerge (mode "replaymerge") or a
 * PersistentSubscription (mode "ps") against the adv-replaymerge stack and reports, as one JSON object per line on
 * stdout, every state transition, failure, retry and the final sequence check of the e2e payload format
 * (seq int64 LE, length int32 LE, fill byte = seq ^ index). A failed ReplayMerge is retried from the position of
 * the last delivered message up to "retries" times, on a freshly connected archive client.
 *
 *   java -cp aeron-all.jar RmOracle.java replaymerge dir=... recordingId=.. sessionId=.. stream=..
 *       replayDestination=.. liveDestination=.. count=.. [position=0] [timeoutMs] [progressTimeoutMs] [retries]
 *   java -cp aeron-all.jar RmOracle.java ps dir=... recordingId=.. stream=.. liveChannel=.. replayChannel=..
 *       replayStream=.. count=.. [position=0] [timeoutMs]
 */

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.archive.client.PersistentSubscriptionListener;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SystemEpochClock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RmOracle
{
    private static final Map<String, String> ARGS = new HashMap<>();

    public static void main(final String[] args) throws Exception
    {
        String mode = null;
        for (final String arg : args)
        {
            final int idx = arg.indexOf('=');
            if (idx < 0)
            {
                mode = arg;
            }
            else
            {
                ARGS.put(arg.substring(0, idx), arg.substring(idx + 1));
            }
        }
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(str("dir", null));
        ctx.unavailableImageHandler(image -> emit("unavailableImage", "sessionId", image.sessionId(),
            "position", image.position()));
        try (Aeron aeron = Aeron.connect(ctx))
        {
            if ("replaymerge".equals(mode))
            {
                replayMerge(aeron);
            }
            else if ("ps".equals(mode))
            {
                persistentSubscription(aeron);
            }
            else
            {
                throw new IllegalArgumentException("unknown mode " + mode);
            }
        }
        catch (final Throwable t)
        {
            emit("fatal", "class", t.getClass().getName(), "message", String.valueOf(t.getMessage()));
            System.exit(1);
        }
    }

    static final class Checker implements FragmentHandler
    {
        long next = lng("from", 0);
        long received;
        long gaps;
        long duplicates;
        long corrupt;
        long lastPosition = lng("position", 0);

        public void onFragment(final DirectBuffer buffer, final int offset, final int length,
            final io.aeron.logbuffer.Header header)
        {
            lastPosition = header.position();
            if (length < 12)
            {
                corrupt++;
                return;
            }
            final long seq = buffer.getLong(offset, java.nio.ByteOrder.LITTLE_ENDIAN);
            final int len = buffer.getInt(offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
            boolean ok = len == length;
            for (int i = 12; ok && i < length; i++)
            {
                ok = buffer.getByte(offset + i) == (byte)(((byte)seq) ^ ((byte)i));
            }
            if (!ok)
            {
                corrupt++;
            }
            received++;
            if (seq < next)
            {
                duplicates++;
                return;
            }
            if (seq > next)
            {
                gaps++;
            }
            next = seq + 1;
        }

        void emitResult(final Object... extra)
        {
            final Object[] base = { "next", next, "received", received, "gaps", gaps, "duplicates", duplicates,
                "corrupt", corrupt, "lastPosition", lastPosition };
            final Object[] all = new Object[base.length + extra.length];
            System.arraycopy(base, 0, all, 0, base.length);
            System.arraycopy(extra, 0, all, base.length, extra.length);
            emit("result", all);
        }
    }

    private static AeronArchive connectArchive(final Aeron aeron, final long deadlineMs)
    {
        while (true)
        {
            try
            {
                return AeronArchive.connect(new AeronArchive.Context()
                    .aeron(aeron)
                    .controlRequestChannel(str("controlChannel", "aeron:udp?endpoint=localhost:27410"))
                    .controlRequestStreamId(10)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:0")
                    .messageTimeoutNs(TimeUnit.SECONDS.toNanos(3)));
            }
            catch (final Exception e)
            {
                emit("archiveConnectError", "class", e.getClass().getName(), "message", String.valueOf(e.getMessage()));
                if (System.currentTimeMillis() > deadlineMs)
                {
                    throw e;
                }
                sleep(200);
            }
        }
    }

    private static String stateOf(final ReplayMerge merge)
    {
        final String s = merge.toString();
        final int start = s.indexOf("state=") + 6;
        return s.substring(start, s.indexOf(',', start));
    }

    private static void replayMerge(final Aeron aeron)
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 60_000);
        final int sessionId = integer("sessionId", 0);
        final int stream = integer("stream", 9400);
        final long count = lng("count", 0);
        final long retries = lng("retries", 0);
        final Checker checker = new Checker();
        final FragmentAssembler assembler = new FragmentAssembler(checker);
        long failures = 0;
        boolean merged = false;
        boolean timedOut = false;
        long position = lng("position", 0);
        emit("ready");
        outer:
        while (true)
        {
            String lastState = null;
            AeronArchive archive = null;
            Subscription sub = null;
            ReplayMerge merge = null;
            try
            {
                archive = connectArchive(aeron, deadline);
                sub = aeron.addSubscription("aeron:udp?control-mode=manual|session-id=" + sessionId, stream);
                merge = new ReplayMerge(sub, archive, "aeron:udp?session-id=" + sessionId,
                    str("replayDestination", null), str("liveDestination", null), lng("recordingId", -1), position,
                    SystemEpochClock.INSTANCE, lng("progressTimeoutMs", 5_000));
                while (checker.next < count)
                {
                    final int n = merge.poll(assembler, 10);
                    final String state = stateOf(merge);
                    if (!state.equals(lastState))
                    {
                        lastState = state;
                        emit("state", "state", state, "next", checker.next);
                    }
                    if (merge.isMerged())
                    {
                        merged = true;
                    }
                    if (System.currentTimeMillis() > deadline)
                    {
                        timedOut = true;
                        break outer;
                    }
                    if (n == 0)
                    {
                        Thread.onSpinWait();
                    }
                }
                break;
            }
            catch (final Exception e)
            {
                failures++;
                emit("failed", "class", e.getClass().getName(), "message", String.valueOf(e.getMessage()),
                    "state", merge == null ? "NONE" : stateOf(merge), "hasFailed", merge != null && merge.hasFailed(),
                    "next", checker.next, "position", checker.lastPosition);
                if (failures > retries || System.currentTimeMillis() > deadline)
                {
                    break;
                }
                position = checker.lastPosition;
            }
            finally
            {
                final long closeStart = System.nanoTime();
                if (merge != null)
                {
                    try
                    {
                        merge.close();
                    }
                    catch (final Exception e)
                    {
                        emit("closeError", "class", e.getClass().getName(), "message", String.valueOf(e.getMessage()));
                    }
                }
                if (sub != null)
                {
                    sub.close();
                }
                if (archive != null)
                {
                    try
                    {
                        archive.close();
                    }
                    catch (final Exception ignore)
                    {
                    }
                }
                emit("closed", "ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStart));
            }
            sleep(lng("retryDelayMs", 500));
        }
        checker.emitResult("merged", merged, "failures", failures, "timedOut", timedOut);
    }

    private static void persistentSubscription(final Aeron aeron)
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 60_000);
        final long count = lng("count", 0);
        final Checker checker = new Checker();
        final long[] joined = new long[1];
        final long[] left = new long[1];
        final PersistentSubscription.Context ctx = new PersistentSubscription.Context()
            .aeron(aeron)
            .recordingId(lng("recordingId", -1))
            .startPosition(lng("position", 0))
            .liveChannel(str("liveChannel", null))
            .liveStreamId(integer("stream", 9400))
            .replayChannel(str("replayChannel", null))
            .replayStreamId(integer("replayStream", 9401))
            .aeronArchiveContext(new AeronArchive.Context()
                .controlRequestChannel(str("controlChannel", "aeron:udp?endpoint=localhost:27410"))
                .controlRequestStreamId(10)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0")
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(3)))
            .listener(new PersistentSubscriptionListener()
            {
                public void onLiveJoined()
                {
                    joined[0]++;
                    emit("liveJoined", "next", checker.next);
                }

                public void onLiveLeft()
                {
                    left[0]++;
                    emit("liveLeft", "next", checker.next);
                }

                public void onError(final Exception e)
                {
                    emit("error", "class", e.getClass().getName(), "message", String.valueOf(e.getMessage()));
                }
            });
        boolean timedOut = false;
        try (PersistentSubscription ps = PersistentSubscription.create(ctx))
        {
            emit("ready");
            final FragmentAssembler assembler = new FragmentAssembler(checker);
            while (checker.next < count && !ps.hasFailed())
            {
                if (ps.poll(assembler, 10) == 0)
                {
                    Thread.onSpinWait();
                }
                if (System.currentTimeMillis() > deadline)
                {
                    timedOut = true;
                    break;
                }
            }
            checker.emitResult("live", ps.isLive(), "failed", ps.hasFailed(), "joined", joined[0], "left", left[0],
                "timedOut", timedOut);
        }
    }

    private static void sleep(final long ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private static String str(final String key, final String def)
    {
        return ARGS.getOrDefault(key, def);
    }

    private static long lng(final String key, final long def)
    {
        return ARGS.containsKey(key) ? Long.parseLong(ARGS.get(key)) : def;
    }

    private static int integer(final String key, final int def)
    {
        return ARGS.containsKey(key) ? Integer.parseInt(ARGS.get(key)) : def;
    }

    static void emit(final String event, final Object... kv)
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
                sb.append('"').append(String.valueOf(v).replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\n", " ")).append('"');
            }
        }
        synchronized (System.out)
        {
            System.out.println(sb.append('}'));
            System.out.flush();
        }
    }
}
