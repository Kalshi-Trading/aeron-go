// Emits golden encodings of io.aeron.command flyweights (Aeron 1.53.3) as "name hex" lines for flyweights_golden_test.go.
// Regenerate: javac -cp aeron-all-1.53.3.jar -d /tmp GoldenBytes.java &&
//   java --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED -cp aeron-all-1.53.3.jar:/tmp GoldenBytes > golden.txt
import io.aeron.ErrorCode;
import io.aeron.command.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class GoldenBytes
{
    static final long CLIENT_ID = 0x0102030405060708L;
    static final long CORRELATION_ID = 0x1112131415161718L;

    static UnsafeBuffer buffer()
    {
        return new UnsafeBuffer(new byte[1024]);
    }

    static void emit(final String name, final UnsafeBuffer buffer, final int length)
    {
        final StringBuilder sb = new StringBuilder(name).append(' ');
        for (int i = 0; i < length; i++)
        {
            sb.append(String.format("%02x", buffer.getByte(i)));
        }
        System.out.println(sb);
    }

    static UnsafeBuffer bytes(final String s)
    {
        return new UnsafeBuffer(s.getBytes(StandardCharsets.US_ASCII));
    }

    public static void main(final String[] args) throws Exception
    {
        final String channel = "aeron:udp?endpoint=localhost:20121";
        UnsafeBuffer b;

        b = buffer();
        new CorrelatedMessageFlyweight().wrap(b, 0).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("CorrelatedMessage", b, CorrelatedMessageFlyweight.LENGTH);

        b = buffer();
        final PublicationMessageFlyweight pub = new PublicationMessageFlyweight();
        pub.wrap(b, 0).streamId(1001).channel(channel).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("PublicationMessage", b, PublicationMessageFlyweight.computeLength(channel.length()));

        b = buffer();
        final SubscriptionMessageFlyweight sub = new SubscriptionMessageFlyweight();
        sub.wrap(b, 0).registrationCorrelationId(-1).streamId(1002).channel(channel)
            .clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("SubscriptionMessage", b, SubscriptionMessageFlyweight.computeLength(channel.length()));

        b = buffer();
        new RemoveSubscriptionFlyweight().wrap(b, 0).registrationId(0x2122232425262728L)
            .clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("RemoveMessage", b, RemoveSubscriptionFlyweight.length());

        b = buffer();
        new RemovePublicationFlyweight().wrap(b, 0).revoke(true).registrationId(0x2122232425262728L)
            .clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("RemovePublicationMessage", b, RemovePublicationFlyweight.length());

        b = buffer();
        new DestinationMessageFlyweight().wrap(b, 0).registrationCorrelationId(0x2122232425262728L)
            .channel(channel).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("DestinationMessage", b, DestinationMessageFlyweight.computeLength(channel.length()));

        b = buffer();
        new DestinationByIdMessageFlyweight().wrap(b, 0).resourceRegistrationId(0x2122232425262728L)
            .destinationRegistrationId(0x3132333435363738L).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("DestinationByIDMessage", b, DestinationByIdMessageFlyweight.MESSAGE_LENGTH);

        final UnsafeBuffer key = bytes("key12");
        final UnsafeBuffer label = bytes("my counter label");

        b = buffer();
        new CounterMessageFlyweight().wrap(b, 0).keyBuffer(key, 0, 5).labelBuffer(label, 0, label.capacity())
            .typeId(1101).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("CounterMessage", b, CounterMessageFlyweight.computeLength(5, label.capacity()));

        b = buffer();
        new CounterMessageFlyweight().wrap(b, 0).keyBuffer(null, 0, 0).label("label only")
            .typeId(1102).clientId(CLIENT_ID).correlationId(CORRELATION_ID);
        emit("CounterMessageLabelOnly", b, CounterMessageFlyweight.computeLength(0, 10));

        b = buffer();
        new StaticCounterMessageFlyweight().wrap(b, 0).keyBuffer(key, 0, 5).labelBuffer(label, 0, label.capacity())
            .typeId(1103).registrationId(0x4142434445464748L).correlationId(CORRELATION_ID).clientId(CLIENT_ID);
        emit("StaticCounterMessage", b, StaticCounterMessageFlyweight.computeLength(5, label.capacity()));

        b = buffer();
        final UnsafeBuffer token = bytes("tok");
        new TerminateDriverFlyweight().wrap(b, 0).tokenBuffer(token, 0, 3).clientId(CLIENT_ID).correlationId(-1);
        emit("TerminateDriverMessage", b, TerminateDriverFlyweight.computeLength(3));

        b = buffer();
        new RejectImageFlyweight().wrap(b, 0).clientId(CLIENT_ID).correlationId(CORRELATION_ID)
            .imageCorrelationId(0x5152535455565758L).position(4096).reason("bad image");
        emit("RejectImageMessage", b, RejectImageFlyweight.computeLength("bad image"));

        b = buffer();
        new GetNextAvailableSessionIdMessageFlyweight().wrap(b, 0).streamId(1003)
            .correlationId(CORRELATION_ID).clientId(CLIENT_ID);
        emit("GetNextAvailableSessionIDMessage", b, GetNextAvailableSessionIdMessageFlyweight.LENGTH);

        // Responses

        b = buffer();
        final ErrorResponseFlyweight err = new ErrorResponseFlyweight();
        err.wrap(b, 0).offendingCommandCorrelationId(CORRELATION_ID).errorCode(ErrorCode.INVALID_CHANNEL)
            .errorMessage("invalid channel");
        emit("ErrorResponse", b, err.length());

        b = buffer();
        final PublicationBuffersReadyFlyweight pubReady = new PublicationBuffersReadyFlyweight();
        pubReady.wrap(b, 0).correlationId(CORRELATION_ID).registrationId(0x2122232425262728L).sessionId(-12345)
            .streamId(1001).publicationLimitCounterId(7).channelStatusCounterId(8).logFileName("/dev/shm/a/1.logbuffer");
        emit("PublicationBuffersReady", b, pubReady.length());

        b = buffer();
        final ImageBuffersReadyFlyweight image = new ImageBuffersReadyFlyweight();
        image.wrap(b, 0).correlationId(CORRELATION_ID).sessionId(-12345).streamId(1002)
            .subscriptionRegistrationId(0x2122232425262728L).subscriberPositionId(9)
            .logFileName("/dev/shm/a/images/2.logbuffer").sourceIdentity("127.0.0.1:20121");
        emit("ImageBuffersReady", b, image.length());

        b = buffer();
        new SubscriptionReadyFlyweight().wrap(b, 0).correlationId(CORRELATION_ID).channelStatusCounterId(10);
        emit("SubscriptionReady", b, SubscriptionReadyFlyweight.LENGTH);

        b = buffer();
        new OperationSucceededFlyweight().wrap(b, 0).correlationId(CORRELATION_ID);
        emit("OperationSucceeded", b, OperationSucceededFlyweight.LENGTH);

        b = buffer();
        final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
        imageMessage.wrap(b, 0).correlationId(CORRELATION_ID).subscriptionRegistrationId(0x2122232425262728L)
            .streamId(1002).channel(channel);
        emit("ImageMessage", b, imageMessage.length());

        b = buffer();
        new CounterUpdateFlyweight().wrap(b, 0).correlationId(CORRELATION_ID).counterId(11);
        emit("CounterUpdate", b, CounterUpdateFlyweight.LENGTH);

        b = buffer();
        new ClientTimeoutFlyweight().wrap(b, 0).clientId(CLIENT_ID);
        emit("ClientTimeout", b, ClientTimeoutFlyweight.LENGTH);

        b = buffer();
        new StaticCounterFlyweight().wrap(b, 0).correlationId(CORRELATION_ID).counterId(12);
        emit("StaticCounter", b, StaticCounterFlyweight.LENGTH);

        b = buffer();
        new NextAvailableSessionIdFlyweight().wrap(b, 0).correlationId(CORRELATION_ID).nextSessionId(-777);
        emit("NextAvailableSessionID", b, NextAvailableSessionIdFlyweight.LENGTH);

        b = buffer();
        final PublicationErrorFrameFlyweight pubErr = new PublicationErrorFrameFlyweight();
        pubErr.wrap(b, 0).registrationId(0x2122232425262728L).destinationRegistrationId(-1).sessionId(-12345)
            .streamId(1001).receiverId(0x6162636465666768L).groupTag(99)
            .sourceAddress(new InetSocketAddress("10.1.2.3", 40123)).errorCode(ErrorCode.IMAGE_REJECTED)
            .errorMessage("rejected");
        emit("PublicationErrorFrame", b, pubErr.length());

        b = buffer();
        pubErr.wrap(b, 0).registrationId(1).destinationRegistrationId(2).sessionId(3).streamId(4).receiverId(5)
            .groupTag(6).sourceAddress(new InetSocketAddress("fe80::1", 65535)).errorCode(ErrorCode.GENERIC_ERROR)
            .errorMessage("v6");
        emit("PublicationErrorFrameIPv6", b, pubErr.length());
    }
}
