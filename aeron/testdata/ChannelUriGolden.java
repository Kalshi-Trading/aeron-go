// Generates channeluri_golden.txt from Java Aeron 1.53.3 for aeron/channeluri_golden_test.go.
//
// java --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED -cp aeron-all-1.53.3.jar \
//     ChannelUriGolden.java < channeluri_inputs.txt > channeluri_golden.txt
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class ChannelUriGolden
{
    public static void main(final String[] args) throws Exception
    {
        final BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while (null != (line = in.readLine()))
        {
            if (line.isEmpty() || line.startsWith("#"))
            {
                continue;
            }
            System.out.println("IN\t" + line);
            System.out.println("PARSE\t" + parse(line));
            System.out.println("BUILD\t" + build(line));
        }
    }

    private static String parse(final String s)
    {
        try
        {
            final ChannelUri uri = ChannelUri.parse(s);
            final TreeMap<String, String> params = new TreeMap<>();
            uri.forEachParameter(params::put);
            final StringBuilder sb = new StringBuilder();
            sb.append(uri.prefix()).append('\t').append(uri.media()).append('\t')
                .append(uri.channelTag()).append('\t').append(uri.entityTag());
            params.forEach((k, v) -> sb.append('\t').append(k).append('=').append(v));
            return sb.toString();
        }
        catch (final RuntimeException ex)
        {
            return "ERR";
        }
    }

    private static String build(final String s)
    {
        try
        {
            return new ChannelUriStringBuilder(s).build();
        }
        catch (final RuntimeException ex)
        {
            return "ERR";
        }
    }
}
