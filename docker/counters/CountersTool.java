import io.aeron.Aeron;
import io.aeron.Counter;

/**
 * Java side of the counters E2E test (systests/counters_e2e_test.go).
 * <pre>
 *   add TYPE_ID LABEL VALUE HOLD_MS            add a client owned counter, set value, hold, then close
 *   add-static TYPE_ID REGISTRATION_ID LABEL   add (or find) a static counter
 * </pre>
 */
public class CountersTool
{
    public static void main(final String[] args) throws Exception
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context()))
        {
            switch (args[0])
            {
                case "add":
                {
                    final Counter counter = aeron.addCounter(Integer.parseInt(args[1]), args[2]);
                    counter.set(Long.parseLong(args[3]));
                    System.out.println("COUNTER id=" + counter.id() + " registrationId=" + counter.registrationId());
                    System.out.flush();
                    Thread.sleep(Long.parseLong(args[4]));
                    counter.close();
                    break;
                }

                case "add-static":
                {
                    final Counter counter = aeron.addStaticCounter(
                        Integer.parseInt(args[1]), args[3], Long.parseLong(args[2]));
                    System.out.println("STATIC id=" + counter.id() + " registrationId=" + counter.registrationId() +
                        " value=" + counter.get());
                    break;
                }

                default:
                    throw new IllegalArgumentException("unknown command: " + args[0]);
            }
        }
    }
}
