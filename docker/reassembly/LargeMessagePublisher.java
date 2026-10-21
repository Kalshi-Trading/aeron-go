/*
 * Publishes a deterministic sequence of (mostly fragmented) messages for the aeron-go reassembly E2E test
 * (systests/reassembly_e2e_test.go). Each data message i is followed by an unfragmented 16 byte "trailer"
 * holding the position returned by offer() for message i, the index i and the length of message i, so the Go side can check the
 * Header.Position() reported for the reassembled message. Every frame's reserved value is
 * (i << 32) | termOffset so the Go side can check the delivered header is that of the first fragment.
 *
 * args: channel streamId messageCount
 */
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.ReservedValueSupplier;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

public final class LargeMessagePublisher
{
    static int lengthFor(final int i, final int maxPayload, final int maxMessage)
    {
        final int[] lengths = {
            1, 31, 32, maxPayload - 1, maxPayload, maxPayload + 1, 2 * maxPayload, 2 * maxPayload + 1,
            4096, 65536, 100_003, 1 << 20, maxMessage, 3 * maxPayload - 7 };
        if (i < lengths.length)
        {
            return Math.min(lengths[i], maxMessage);
        }
        final long x = (i * 2654435761L) & 0x7FFFFFFFL;
        return 1 + (int)(x % Math.min(maxMessage, 300_000));
    }

    static byte patternByte(final int i, final int j)
    {
        return (byte)(i * 31 + j * 7 + (j >> 8));
    }

    public static void main(final String[] args) throws Exception
    {
        final String channel = args[0];
        final int streamId = Integer.parseInt(args[1]);
        final int count = Integer.parseInt(args[2]);

        try (Aeron aeron = Aeron.connect(new Aeron.Context());
            Publication pub = aeron.addPublication(channel, streamId))
        {
            final long deadline = System.nanoTime() + 30_000_000_000L;
            while (!pub.isConnected())
            {
                if (System.nanoTime() > deadline)
                {
                    throw new IllegalStateException("not connected: " + channel);
                }
                Thread.sleep(10);
            }

            final int maxPayload = pub.maxPayloadLength();
            final int maxMessage = pub.maxMessageLength();
            System.out.println("connected sessionId=" + pub.sessionId() + " maxPayload=" + maxPayload +
                " maxMessage=" + maxMessage + " termLength=" + pub.termBufferLength());

            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(maxMessage, 64));
            final UnsafeBuffer trailer = new UnsafeBuffer(new byte[16]);
            long totalBytes = 0;

            for (int i = 0; i < count; i++)
            {
                final int length = lengthFor(i, maxPayload, maxMessage);
                for (int j = 0; j < length; j++)
                {
                    buffer.putByte(j, patternByte(i, j));
                }

                final long index = i;
                final ReservedValueSupplier supplier =
                    (termBuffer, termOffset, frameLength) -> (index << 32) | (termOffset & 0xFFFF_FFFFL);

                final long position = offer(pub, buffer, length, supplier);
                trailer.putLong(0, position);
                trailer.putInt(8, i);
                trailer.putInt(12, length);
                offer(pub, trailer, trailer.capacity(), supplier);
                totalBytes += length;
            }

            System.out.println("sent messages=" + count + " bytes=" + totalBytes + " position=" + pub.position());
            Thread.sleep(2000);
        }
    }

    private static long offer(
        final Publication pub, final UnsafeBuffer buffer, final int length, final ReservedValueSupplier supplier)
        throws InterruptedException
    {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (true)
        {
            final long result = pub.offer(buffer, 0, length, supplier);
            if (result > 0)
            {
                return result;
            }
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED ||
                System.nanoTime() > deadline)
            {
                throw new IllegalStateException("offer failed: " + result);
            }
            Thread.yield();
        }
    }
}
