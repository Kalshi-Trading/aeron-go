/*
 * Java 1.53.3 subscriber used by the aeron-go publication E2E tests (systests/publications_e2e_test.go).
 * Subscribes to each configured stream on a channel, reassembles fragmented messages and replies, for every
 * message, with a summary line (length, CRC32, reserved value, header position/term info) on a reply channel.
 */
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

public class PubVerifier
{
    public static void main(final String[] args)
    {
        final String channel = System.getProperty("verifier.channel", "aeron:udp?endpoint=localhost:20140");
        final String streams = System.getProperty("verifier.streams", "3001,3002,3003,3004,3005,3006");
        final String replyChannel = System.getProperty("verifier.reply.channel", "aeron:udp?endpoint=localhost:20141");
        final int replyStreamId = Integer.getInteger("verifier.reply.stream", 3100);
        final AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try (Aeron aeron = Aeron.connect(new Aeron.Context());
            Publication reply = aeron.addPublication(replyChannel, replyStreamId))
        {
            final List<Subscription> subscriptions = new ArrayList<>();
            final List<FragmentAssembler> assemblers = new ArrayList<>();
            for (final String s : streams.split(","))
            {
                final int streamId = Integer.parseInt(s.trim());
                subscriptions.add(aeron.addSubscription(channel, streamId,
                    (image) -> System.out.println("available stream=" + streamId + " session=" + image.sessionId() +
                        " joinPosition=" + image.joinPosition() + " initialTermId=" + image.initialTermId() +
                        " termLength=" + image.termBufferLength()),
                    (image) -> System.out.println("unavailable stream=" + streamId + " session=" + image.sessionId())));
                assemblers.add(new FragmentAssembler(handler(streamId, reply, running)));
            }
            System.out.println("PubVerifier ready channel=" + channel + " streams=" + streams +
                " reply=" + replyChannel + " stream=" + replyStreamId);

            final IdleStrategy idle = new SleepingMillisIdleStrategy(1);
            while (running.get())
            {
                int work = 0;
                for (int i = 0; i < subscriptions.size(); i++)
                {
                    work += subscriptions.get(i).poll(assemblers.get(i), 16);
                }
                idle.idle(work);
            }
        }
    }

    private static FragmentHandler handler(final int streamId, final Publication reply, final AtomicBoolean running)
    {
        return (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final CRC32 crc = new CRC32();
            crc.update(bytes);
            final String summary = String.format(
                "stream=%d session=%d len=%d crc=%d rv=%d pos=%d termId=%d termOffset=%d initialTermId=%d",
                streamId, header.sessionId(), length, crc.getValue(), header.reservedValue(), header.position(),
                header.termId(), header.termOffset(), header.initialTermId());
            System.out.println(summary);

            final UnsafeBuffer msg = new UnsafeBuffer(summary.getBytes(StandardCharsets.US_ASCII));
            final long deadline = System.nanoTime() + 5_000_000_000L;
            while (running.get() && reply.offer(msg) < 0 && System.nanoTime() < deadline)
            {
                Thread.onSpinWait();
            }
        };
    }
}
