import io.aeron.LogBuffers;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

// Prints every io.aeron.logbuffer.LogBufferDescriptor metadata field of a log buffer as key=value lines.
public class LogMetaDump
{
    public static void main(final String[] args)
    {
        try (LogBuffers lb = new LogBuffers(args[0], true))
        {
            final UnsafeBuffer m = lb.metaDataBuffer();
            p("termLength", termLength(m));
            p("initialTermId", initialTermId(m));
            p("mtuLength", mtuLength(m));
            p("pageSize", pageSize(m));
            p("correlationId", correlationId(m));
            p("isConnected", isConnected(m));
            p("activeTransportCount", activeTransportCount(m));
            p("endOfStreamPosition", endOfStreamPosition(m));
            p("activeTermCount", activeTermCount(m));
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                p("rawTail" + i, rawTailVolatile(m, i));
            }
            p("sparse", sparse(m));
            p("tether", tether(m));
            p("isPublicationRevoked", isPublicationRevoked(m));
            p("type", type(m));
            p("group", group(m));
            p("isResponse", isResponse(m));
            p("rejoin", rejoin(m));
            p("reliable", reliable(m));
            p("socketRcvbufLength", socketRcvbufLength(m));
            p("osDefaultSocketRcvbufLength", osDefaultSocketRcvbufLength(m));
            p("osMaxSocketRcvbufLength", osMaxSocketRcvbufLength(m));
            p("socketSndbufLength", socketSndbufLength(m));
            p("osDefaultSocketSndbufLength", osDefaultSocketSndbufLength(m));
            p("osMaxSocketSndbufLength", osMaxSocketSndbufLength(m));
            p("receiverWindowLength", receiverWindowLength(m));
            p("publicationWindowLength", publicationWindowLength(m));
            p("untetheredWindowLimitTimeoutNs", untetheredWindowLimitTimeoutNs(m));
            p("untetheredLingerTimeoutNs", untetheredLingerTimeoutNs(m));
            p("untetheredRestingTimeoutNs", untetheredRestingTimeoutNs(m));
            p("maxResend", maxResend(m));
            p("lingerTimeoutNs", lingerTimeoutNs(m));
            p("entityTag", entityTag(m));
            p("responseCorrelationId", responseCorrelationId(m));
            p("signalEos", signalEos(m));
            p("spiesSimulateConnection", spiesSimulateConnection(m));
        }
    }

    private static void p(final String key, final Object value)
    {
        System.out.println(key + "=" + value);
    }
}
