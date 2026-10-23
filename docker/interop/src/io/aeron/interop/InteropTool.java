/*
 * Java side of the aeron-go cross-language interop tests (see interop/ in the Go module).
 *
 * Usage: InteropTool <mode> key=value...
 *   modes: pub, sub, record, replay, replaymerge, describe, respserver, respclient
 *
 * Every mode prints one JSON object per line on stdout ("event" names the line type). Message i of a
 * run is generated deterministically, identically to interop/payload.go:
 *   length(i)   = sizes[i % sizes.length]
 *   bytes[0..8) = little-endian i,  bytes[j] = (byte)(i * 31 + j) for j >= 8
 *   reserved(i) = (i << 32) | 0xCAFE
 * Receivers verify every message and report a SHA-256 over all received payload bytes.
 */
package io.aeron.interop;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayMerge;
import io.aeron.archive.client.ReplayParams;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class InteropTool
{
    private static final Map<String, String> ARGS = new HashMap<>();
    private static final List<String> DESTS = new ArrayList<>();
    private static final BlockingQueue<String> STDIN = new LinkedBlockingQueue<>();

    public static void main(final String[] args) throws Exception
    {
        if (args.length < 1)
        {
            throw new IllegalArgumentException("usage: InteropTool <mode> key=value...");
        }
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

        final Aeron.Context ctx = new Aeron.Context();
        if (ARGS.containsKey("dir"))
        {
            ctx.aeronDirectoryName(ARGS.get("dir"));
        }
        ctx.availableImageHandler(InteropTool::onAvailableImage);
        ctx.unavailableImageHandler(InteropTool::onUnavailableImage);
        try (Aeron aeron = Aeron.connect(ctx))
        {
            switch (args[0])
            {
                case "pub": pub(aeron); break;
                case "sub": sub(aeron); break;
                case "record": record(aeron); break;
                case "replay": replay(aeron); break;
                case "replaymerge": replayMerge(aeron); break;
                case "describe": describe(aeron); break;
                case "respserver": responseServer(aeron); break;
                case "respclient": responseClient(aeron); break;
                default: throw new IllegalArgumentException("unknown mode " + args[0]);
            }
        }
        catch (final Throwable t)
        {
            emit("error", "message", String.valueOf(t));
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    // ----------------------------------------------------------------------------------------------------------- pub

    private static void pub(final Aeron aeron) throws Exception
    {
        final String channel = str("channel", null);
        final int stream = integer("stream", 1001);
        final boolean exclusive = bool("exclusive", false);
        final Publication pub = exclusive ?
            aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream);
        for (final String d : DESTS)
        {
            pub.addDestination(d);
        }
        emit("ready", "sessionId", pub.sessionId(), "streamId", pub.streamId(),
            "maxPayloadLength", pub.maxPayloadLength(), "maxMessageLength", pub.maxMessageLength(),
            "registrationId", pub.registrationId());

        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        while (!pub.isConnected())
        {
            if (System.currentTimeMillis() > deadline)
            {
                throw new IllegalStateException("publication not connected");
            }
            Thread.sleep(1);
        }
        emit("connected", "sessionId", pub.sessionId());
        if (bool("waitGo", false))
        {
            waitStdin("go", deadline);
        }
        final long t0 = System.nanoTime();
        final long position = publish(pub, deadline);
        emit("done", "position", position, "count", lng("count", 0),
            "durationMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0));
        waitStdin("exit", System.currentTimeMillis() + lng("lingerMs", 60_000));
        pub.close();
    }

    private static long publish(final Publication pub, final long deadline)
    {
        final long count = lng("count", 0);
        final int[] sizes = sizes();
        final boolean claim = "claim".equals(str("mode", "offer"));
        final boolean reserved = bool("reserved", false);
        final long delayUs = lng("delayUs", 0);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[maxSize(sizes)]);
        final BufferClaim bufferClaim = new BufferClaim();
        for (long i = 0; i < count; i++)
        {
            final int length = sizes[(int)(i % sizes.length)];
            final long rv = reservedValue(i);
            while (true)
            {
                final long result;
                if (claim)
                {
                    result = pub.tryClaim(length, bufferClaim);
                    if (result > 0)
                    {
                        fill(bufferClaim.buffer(), bufferClaim.offset(), i, length);
                        if (reserved)
                        {
                            bufferClaim.reservedValue(rv);
                        }
                        bufferClaim.commit();
                    }
                }
                else
                {
                    fill(buffer, 0, i, length);
                    result = reserved ?
                        pub.offer(buffer, 0, length, (termBuffer, termOffset, frameLength) -> rv) :
                        pub.offer(buffer, 0, length);
                }
                if (result > 0)
                {
                    break;
                }
                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED)
                {
                    throw new IllegalStateException("offer failed: " + result);
                }
                if (System.currentTimeMillis() > deadline)
                {
                    throw new IllegalStateException("offer timed out at message " + i + " result=" + result);
                }
                Thread.onSpinWait();
            }
            if (delayUs > 0)
            {
                LockSupport.parkNanos(delayUs * 1000);
            }
        }
        return pub.position();
    }

    // ----------------------------------------------------------------------------------------------------------- sub

    private static void sub(final Aeron aeron) throws Exception
    {
        final Subscription sub = aeron.addSubscription(str("channel", null), integer("stream", 1001));
        for (final String d : DESTS)
        {
            sub.addDestination(d);
        }
        emit("ready", "registrationId", sub.registrationId());
        final Verifier verifier = new Verifier();
        final FragmentHandler handler = bool("assembler", true) ? new FragmentAssembler(verifier) : verifier;
        final long count = lng("count", 0);
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        final long delayUs = lng("delayUs", 0);
        final int limit = delayUs > 0 ? 1 : 10;
        boolean timedOut = false;
        while (verifier.count < count)
        {
            if (sub.poll(handler, limit) == 0)
            {
                Thread.onSpinWait();
            }
            else if (delayUs > 0)
            {
                LockSupport.parkNanos(delayUs * 1000);
            }
            if (System.currentTimeMillis() > deadline)
            {
                timedOut = true;
                break;
            }
        }
        verifier.emitResult(timedOut);
    }

    // -------------------------------------------------------------------------------------------------------- record

    private static AeronArchive connectArchive(final Aeron aeron)
    {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(str("controlChannel", "aeron:udp?endpoint=localhost:8010"))
            .controlRequestStreamId(integer("controlStream", 10))
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:8030"));
    }

    private static void record(final Aeron aeron) throws Exception
    {
        final String channel = str("channel", null);
        final int stream = integer("stream", 1001);
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        try (AeronArchive archive = connectArchive(aeron))
        {
            final Publication pub = bool("exclusive", false) ?
                aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream);
            final long subscriptionId = archive.startRecording(
                ChannelUri.addSessionId(channel, pub.sessionId()), stream, SourceLocation.LOCAL);
            final CountersReader counters = aeron.countersReader();
            int counterId;
            while (CountersReader.NULL_COUNTER_ID ==
                (counterId = RecordingPos.findCounterIdBySession(counters, pub.sessionId(), archive.archiveId())))
            {
                check(deadline, "recording counter");
                Thread.sleep(1);
            }
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);
            while (!pub.isConnected())
            {
                check(deadline, "publication connected");
                Thread.sleep(1);
            }
            final long position = publish(pub, deadline);
            while (counters.getCounterValue(counterId) < position)
            {
                check(deadline, "recording position");
                Thread.sleep(1);
            }
            archive.stopRecording(subscriptionId);
            emit("recorded", "recordingId", recordingId, "stopPosition", position,
                "sessionId", pub.sessionId(), "count", lng("count", 0));
            pub.close();
        }
    }

    // -------------------------------------------------------------------------------------------------------- replay

    private static void replay(final Aeron aeron) throws Exception
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        try (AeronArchive archive = connectArchive(aeron);
            Subscription sub = archive.replay(
                lng("recordingId", -1), str("channel", "aeron:udp?endpoint=localhost:0"), integer("stream", 2001),
                new ReplayParams().position(lng("position", 0)).length(lng("length", AeronArchive.NULL_LENGTH))))
        {
            emit("ready", "registrationId", sub.registrationId());
            final Verifier verifier = new Verifier();
            final FragmentAssembler assembler = new FragmentAssembler(verifier);
            final long count = lng("count", 0);
            boolean timedOut = false;
            while (verifier.count < count)
            {
                if (sub.poll(assembler, 10) == 0)
                {
                    Thread.onSpinWait();
                }
                if (System.currentTimeMillis() > deadline)
                {
                    timedOut = true;
                    break;
                }
            }
            verifier.emitResult(timedOut);
        }
    }

    // --------------------------------------------------------------------------------------------------- replaymerge

    private static void replayMerge(final Aeron aeron) throws Exception
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        final int sessionId = integer("sessionId", 0);
        final int stream = integer("stream", 1001);
        final String subscriptionChannel = "aeron:udp?control-mode=manual|session-id=" + sessionId;
        final String replayChannel = "aeron:udp?session-id=" + sessionId;
        final String replayDestination = str("replayDestination", "aeron:udp?endpoint=localhost:0");
        final String liveDestination = str("liveDestination", null);
        try (AeronArchive archive = connectArchive(aeron);
            Subscription sub = aeron.addSubscription(subscriptionChannel, stream);
            ReplayMerge replayMerge = new ReplayMerge(
                sub, archive, replayChannel, replayDestination, liveDestination,
                lng("recordingId", -1), lng("position", 0)))
        {
            emit("ready", "registrationId", sub.registrationId());
            final Verifier verifier = new Verifier();
            final FragmentAssembler assembler = new FragmentAssembler(verifier);
            final long count = lng("count", 0);
            boolean timedOut = false;
            boolean merged = false;
            while (verifier.count < count)
            {
                if (replayMerge.poll(assembler, 10) == 0)
                {
                    Thread.onSpinWait();
                }
                if (!merged && replayMerge.isMerged())
                {
                    merged = true;
                    emit("merged", "received", verifier.count);
                }
                if (replayMerge.hasFailed())
                {
                    throw new IllegalStateException("replay merge failed: " + replayMerge);
                }
                if (System.currentTimeMillis() > deadline)
                {
                    timedOut = true;
                    break;
                }
            }
            emit("state", "merged", replayMerge.isMerged(), "liveAdded", replayMerge.isLiveAdded());
            verifier.emitResult(timedOut);
        }
    }

    // ------------------------------------------------------------------------------------------------------ describe

    private static void describe(final Aeron aeron)
    {
        try (AeronArchive archive = connectArchive(aeron))
        {
            final int found = archive.listRecording(lng("recordingId", -1),
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) -> emit("descriptor",
                "recordingId", recordingId, "startTimestamp", startTimestamp, "stopTimestamp", stopTimestamp,
                "startPosition", startPosition, "stopPosition", stopPosition, "initialTermId", initialTermId,
                "segmentFileLength", segmentFileLength, "termBufferLength", termBufferLength,
                "mtuLength", mtuLength, "sessionId", sessionId, "streamId", streamId,
                "strippedChannel", strippedChannel, "originalChannel", originalChannel,
                "sourceIdentity", sourceIdentity));
            emit("described", "found", found);
        }
    }

    // ------------------------------------------------------------------------------------------ response channels

    /**
     * Response server: subscribes to channel/stream, and for the first request image adds a response publication
     * on respChannel|response-correlation-id=image.correlationId(). Verifies count request messages, then publishes
     * count response messages.
     */
    private static void responseServer(final Aeron aeron) throws Exception
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        final Subscription sub = aeron.addSubscription(str("channel", null), integer("stream", 1001));
        emit("ready", "registrationId", sub.registrationId());
        while (sub.imageCount() == 0)
        {
            check(deadline, "request image");
            Thread.sleep(1);
        }
        final Image image = sub.imageAtIndex(0);
        final String respChannel = withParam(str("respChannel", null), "response-correlation-id", image.correlationId());
        final int respStream = integer("respStream", 1002);
        final Publication pub = bool("exclusive", false) ?
            aeron.addExclusivePublication(respChannel, respStream) : aeron.addPublication(respChannel, respStream);
        emit("request", "correlationId", image.correlationId(), "sessionId", image.sessionId(),
            "responseSessionId", pub.sessionId());
        while (!pub.isConnected())
        {
            check(deadline, "response publication connected");
            Thread.sleep(1);
        }
        emit("connected", "sessionId", pub.sessionId());
        final Verifier verifier = new Verifier();
        final FragmentAssembler assembler = new FragmentAssembler(verifier);
        final long count = lng("count", 0);
        boolean timedOut = false;
        while (verifier.count < count)
        {
            if (image.poll(assembler, 10) == 0)
            {
                Thread.onSpinWait();
            }
            if (System.currentTimeMillis() > deadline)
            {
                timedOut = true;
                break;
            }
        }
        verifier.emitResult(timedOut);
        final long position = publish(pub, deadline);
        emit("done", "position", position, "count", count);
        waitStdin("exit", System.currentTimeMillis() + lng("lingerMs", 60_000));
        pub.close();
        sub.close();
    }

    /**
     * Response client: subscribes to respChannel/respStream (control-mode=response) and publishes count requests on
     * channel|response-correlation-id=sub.registrationId(), then verifies count responses.
     */
    private static void responseClient(final Aeron aeron) throws Exception
    {
        final long deadline = System.currentTimeMillis() + lng("timeoutMs", 30_000);
        final Subscription sub = aeron.addSubscription(str("respChannel", null), integer("respStream", 1002));
        final String reqChannel = withParam(str("channel", null), "response-correlation-id", sub.registrationId());
        final int reqStream = integer("stream", 1001);
        final Publication pub = bool("exclusive", false) ?
            aeron.addExclusivePublication(reqChannel, reqStream) : aeron.addPublication(reqChannel, reqStream);
        emit("ready", "registrationId", sub.registrationId(), "sessionId", pub.sessionId());
        while (!pub.isConnected())
        {
            check(deadline, "request publication connected");
            Thread.sleep(1);
        }
        emit("connected", "sessionId", pub.sessionId());
        final long position = publish(pub, deadline);
        emit("done", "position", position, "count", lng("count", 0));
        final Verifier verifier = new Verifier();
        final FragmentAssembler assembler = new FragmentAssembler(verifier);
        final long count = lng("count", 0);
        boolean timedOut = false;
        while (verifier.count < count)
        {
            if (sub.poll(assembler, 10) == 0)
            {
                Thread.onSpinWait();
            }
            if (System.currentTimeMillis() > deadline)
            {
                timedOut = true;
                break;
            }
        }
        verifier.emitResult(timedOut);
        pub.close();
        sub.close();
    }

    // ------------------------------------------------------------------------------------------------------- helpers

    static final class Verifier implements FragmentHandler
    {
        final int[] sizes = sizes();
        final boolean checkReserved = bool("reserved", false);
        final long first = lng("first", 0);
        final MessageDigest digest;
        long count;
        long errors;
        String firstError = "";
        long lastPosition;
        int sessionId;
        long bytes;
        byte[] scratch = new byte[1024];

        Verifier() throws Exception
        {
            digest = MessageDigest.getInstance("SHA-256");
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final long expectedSeq = first + count;
            final String err = verify(buffer, offset, length, expectedSeq, sizes);
            if (err != null)
            {
                error(err);
            }
            else if (checkReserved && header.reservedValue() != reservedValue(expectedSeq))
            {
                error("seq " + expectedSeq + " reserved value " + header.reservedValue() +
                    " != " + reservedValue(expectedSeq));
            }
            if (scratch.length < length)
            {
                scratch = new byte[length];
            }
            buffer.getBytes(offset, scratch, 0, length);
            digest.update(scratch, 0, length);
            bytes += length;
            count++;
            lastPosition = header.position();
            sessionId = header.sessionId();
        }

        private void error(final String err)
        {
            if (errors++ == 0)
            {
                firstError = err;
            }
        }

        void emitResult(final boolean timedOut)
        {
            emit("result", "count", count, "errors", errors, "firstError", firstError,
                "sha256", hex(digest.digest()), "bytes", bytes, "position", lastPosition,
                "sessionId", sessionId, "timedOut", timedOut);
        }
    }

    static String verify(
        final DirectBuffer buffer, final int offset, final int length, final long expectedSeq, final int[] sizes)
    {
        if (length < BitUtil.SIZE_OF_LONG)
        {
            return "seq " + expectedSeq + " short message length " + length;
        }
        final long seq = buffer.getLong(offset, ByteOrder.LITTLE_ENDIAN);
        if (seq != expectedSeq)
        {
            return "expected seq " + expectedSeq + " got " + seq;
        }
        final int expectedLength = sizes[(int)(seq % sizes.length)];
        if (length != expectedLength)
        {
            return "seq " + seq + " length " + length + " != " + expectedLength;
        }
        for (int j = BitUtil.SIZE_OF_LONG; j < length; j++)
        {
            if (buffer.getByte(offset + j) != (byte)(seq * 31 + j))
            {
                return "seq " + seq + " payload mismatch at byte " + j;
            }
        }
        return null;
    }

    static void fill(final org.agrona.MutableDirectBuffer buffer, final int offset, final long seq, final int length)
    {
        buffer.putLong(offset, seq, ByteOrder.LITTLE_ENDIAN);
        for (int j = BitUtil.SIZE_OF_LONG; j < length; j++)
        {
            buffer.putByte(offset + j, (byte)(seq * 31 + j));
        }
    }

    static long reservedValue(final long seq)
    {
        return (seq << 32) | 0xCAFEL;
    }

    static int[] sizes()
    {
        final String[] parts = str("sizes", "64").split(",");
        final int[] sizes = new int[parts.length];
        for (int i = 0; i < parts.length; i++)
        {
            sizes[i] = Integer.parseInt(parts[i].trim());
        }
        return sizes;
    }

    static int maxSize(final int[] sizes)
    {
        int max = 0;
        for (final int s : sizes)
        {
            max = Math.max(max, s);
        }
        return max;
    }

    private static void onAvailableImage(final Image image)
    {
        emit("image", "sessionId", image.sessionId(), "streamId", image.subscription().streamId(),
            "source", image.sourceIdentity(), "joinPosition", image.joinPosition(),
            "correlationId", image.correlationId());
    }

    private static void onUnavailableImage(final Image image)
    {
        emit("imageGone", "sessionId", image.sessionId(), "position", image.position());
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
            STDIN.add("EOF");
        });
        t.setDaemon(true);
        t.start();
    }

    private static void waitStdin(final String token, final long deadline) throws InterruptedException
    {
        while (true)
        {
            final long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0)
            {
                return;
            }
            final String line = STDIN.poll(remaining, TimeUnit.MILLISECONDS);
            if (line == null || line.equals(token) || line.equals("EOF") || line.equals("exit"))
            {
                if ("EOF".equals(line))
                {
                    STDIN.add("EOF");
                }
                return;
            }
        }
    }

    private static void check(final long deadline, final String what)
    {
        if (System.currentTimeMillis() > deadline)
        {
            throw new IllegalStateException("timed out waiting for " + what);
        }
    }

    static String withParam(final String channel, final String key, final Object value)
    {
        return channel + (channel.indexOf('?') >= 0 ? "|" : "?") + key + "=" + value;
    }

    static String hex(final byte[] bytes)
    {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes)
        {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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

    static String str(final String key, final String dflt)
    {
        final String v = ARGS.getOrDefault(key, dflt);
        if (v == null)
        {
            throw new IllegalArgumentException("missing " + key + "=");
        }
        return v;
    }

    static int integer(final String key, final int dflt)
    {
        return Integer.parseInt(str(key, String.valueOf(dflt)));
    }

    static long lng(final String key, final long dflt)
    {
        return Long.parseLong(str(key, String.valueOf(dflt)));
    }

    static boolean bool(final String key, final boolean dflt)
    {
        return Boolean.parseBoolean(str(key, String.valueOf(dflt)));
    }
}
