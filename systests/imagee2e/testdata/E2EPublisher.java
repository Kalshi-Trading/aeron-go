import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

/**
 * Publisher used by the Go image/subscription E2E test. Usage: channel streamId messageCount messageLength.
 * Publishes messageCount messages once connected, then waits for a "close" line on stdin, closes the
 * publication and exits. Error frames from rejected images are printed as "REJECTED: reason".
 */
public class E2EPublisher
{
    public static void main(final String[] args) throws Exception
    {
        final String channel = args[0];
        final int streamId = Integer.parseInt(args[1]);
        final int count = Integer.parseInt(args[2]);
        final int length = Integer.parseInt(args[3]);

        final Aeron.Context ctx = new Aeron.Context()
            .publicationErrorFrameHandler(frame ->
                System.out.println("REJECTED: " + frame.errorMessage()));

        try (Aeron aeron = Aeron.connect(ctx))
        {
            final Publication publication = aeron.addPublication(channel, streamId);
            while (!publication.isConnected())
            {
                Thread.sleep(1);
            }
            System.out.println("CONNECTED " + publication.sessionId());

            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(length));
            for (int i = 0; i < count; i++)
            {
                buffer.setMemory(0, length, (byte)' ');
                buffer.putStringWithoutLengthAscii(0, String.format("e2e-%05d", i));
                while (publication.offer(buffer, 0, length) < 0)
                {
                    Thread.sleep(1);
                }
            }
            System.out.println("PUBLISHED " + publication.position());

            final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while (null != (line = reader.readLine()) && !"close".equals(line))
            {
                // wait
            }
            publication.close();
            System.out.println("CLOSED");
            Thread.sleep(500);
        }
    }
}
