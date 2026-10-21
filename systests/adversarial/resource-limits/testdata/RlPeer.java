// Java Aeron 1.53.3 reference peer for the resource-limits tests: performs the same deterministic offer/claim
// sequence the Go test performs and prints the run-length encoded result codes so they can be compared verbatim.
// Run with: java -cp aeron-all-1.53.3.jar RlPeer.java dir=... channel=... stream=... [exclusive=true]
//           [mode=offer|claim|block] size=... attempts=...
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.HashMap;
import java.util.Map;

public class RlPeer
{
    public static void main(final String[] args) throws Exception
    {
        final Map<String, String> a = new HashMap<>();
        for (final String arg : args)
        {
            final int i = arg.indexOf('=');
            a.put(arg.substring(0, i), arg.substring(i + 1));
        }
        final String channel = a.get("channel");
        final int stream = Integer.parseInt(a.get("stream"));
        final boolean exclusive = Boolean.parseBoolean(a.getOrDefault("exclusive", "false"));
        final String mode = a.getOrDefault("mode", "offer");
        final int size = Integer.parseInt(a.get("size"));
        final int attempts = Integer.parseInt(a.get("attempts"));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(a.get("dir")));
            Subscription sub = aeron.addSubscription(channel, stream);
            Publication pub = exclusive ?
                aeron.addExclusivePublication(channel, stream) : aeron.addPublication(channel, stream))
        {
            final long deadline = System.currentTimeMillis() + 20_000;
            while (!pub.isConnected() || sub.imageCount() == 0)
            {
                if (System.currentTimeMillis() > deadline)
                {
                    throw new IllegalStateException("not connected");
                }
                Thread.sleep(1);
            }
            System.out.println("start position=" + pub.position() + " maxPossiblePosition=" +
                pub.maxPossiblePosition());
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[size]);
            final BufferClaim claim = new BufferClaim();
            final StringBuilder out = new StringBuilder();
            String last = null;
            int run = 0;
            for (int n = 0; n < attempts; )
            {
                final long r;
                switch (mode)
                {
                    case "claim":
                        r = pub.tryClaim(size, claim);
                        if (r > 0)
                        {
                            claim.commit();
                        }
                        break;
                    case "block":
                        r = blockOffer((ExclusivePublication)pub, buffer, size);
                        break;
                    default:
                        r = pub.offer(buffer, 0, size);
                }
                sub.poll((b, o, l, h) -> {}, 100);
                if (r == Publication.BACK_PRESSURED || r == Publication.NOT_CONNECTED)
                {
                    if (System.currentTimeMillis() > deadline)
                    {
                        throw new IllegalStateException("stuck " + Publication.errorString(r) + " n=" + n +
                            " position=" + pub.position() + " limit=" + pub.positionLimit() + " imagePosition=" +
                            sub.imageAtIndex(0).position());
                    }
                    Thread.sleep(1);
                    continue;
                }
                n++;
                final String code = r > 0 ? "OK" : Publication.errorString(r);
                if (!code.equals(last))
                {
                    if (last != null)
                    {
                        out.append(last).append('*').append(run).append(' ');
                    }
                    last = code;
                    run = 0;
                }
                run++;
            }
            out.append(last).append('*').append(run);
            System.out.println("result " + out);
            final long end = System.currentTimeMillis() + 10_000;
            while (sub.imageAtIndex(0).position() < pub.position() && System.currentTimeMillis() < end)
            {
                sub.poll((b, o, l, h) -> {}, 100);
            }
            System.out.println("end position=" + pub.position() + " imagePosition=" + sub.imageAtIndex(0).position());
        }
    }

    // OfferBlock of one pre-framed data frame of `size` bytes (header + payload).
    private static long blockOffer(final ExclusivePublication pub, final UnsafeBuffer buffer, final int size)
    {
        int termOffset = pub.termOffset();
        int termId = pub.termId();
        if (termOffset >= pub.termBufferLength())
        {
            termOffset = 0;
            termId++;
        }
        buffer.putInt(0, size);
        buffer.putByte(DataHeaderFlyweight.VERSION_FIELD_OFFSET, (byte)0);
        buffer.putByte(DataHeaderFlyweight.FLAGS_FIELD_OFFSET, (byte)FrameDescriptor.UNFRAGMENTED);
        buffer.putShort(DataHeaderFlyweight.TYPE_FIELD_OFFSET, (short)DataHeaderFlyweight.HDR_TYPE_DATA);
        buffer.putInt(DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET, termOffset);
        buffer.putInt(DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET, pub.sessionId());
        buffer.putInt(DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET, pub.streamId());
        buffer.putInt(DataHeaderFlyweight.TERM_ID_FIELD_OFFSET, termId);
        return pub.offerBlock(buffer, 0, size);
    }
}
