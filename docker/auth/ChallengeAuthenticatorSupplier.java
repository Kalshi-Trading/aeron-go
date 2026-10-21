import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.SessionProxy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Test authenticator for the aeron-go archive connect E2E tests.
 * <ul>
 *   <li>credentials "admin:admin" are authenticated immediately</li>
 *   <li>credentials "admin:challenge" are challenged with "challenge!" and authenticated on response
 *   "admin:response"</li>
 *   <li>anything else is rejected</li>
 * </ul>
 */
public final class ChallengeAuthenticatorSupplier implements AuthenticatorSupplier
{
    public static final byte[] CHALLENGE = "challenge!".getBytes(StandardCharsets.US_ASCII);

    public Authenticator get()
    {
        return new ChallengeAuthenticator();
    }

    static final class ChallengeAuthenticator implements Authenticator
    {
        private enum Status { ACCEPT, CHALLENGE, CHALLENGE_OK, REJECT }

        private final Map<Long, Status> sessions = new HashMap<>();

        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
            final String creds = new String(encodedCredentials, StandardCharsets.US_ASCII);
            final Status status = "admin:admin".equals(creds) ? Status.ACCEPT :
                "admin:challenge".equals(creds) ? Status.CHALLENGE : Status.REJECT;
            sessions.put(sessionId, status);
        }

        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
            final String creds = new String(encodedCredentials, StandardCharsets.US_ASCII);
            sessions.put(sessionId, "admin:response".equals(creds) ? Status.CHALLENGE_OK : Status.REJECT);
        }

        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            final Status status = sessions.get(sessionProxy.sessionId());
            if (status == Status.ACCEPT)
            {
                sessions.remove(sessionProxy.sessionId());
                sessionProxy.authenticate(null);
            }
            else if (status == Status.CHALLENGE)
            {
                sessionProxy.challenge(CHALLENGE);
            }
            else
            {
                sessions.remove(sessionProxy.sessionId());
                sessionProxy.reject();
            }
        }

        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
        {
            final Status status = sessions.get(sessionProxy.sessionId());
            if (status == Status.CHALLENGE_OK)
            {
                sessions.remove(sessionProxy.sessionId());
                sessionProxy.authenticate(null);
            }
            else if (status == Status.REJECT)
            {
                sessions.remove(sessionProxy.sessionId());
                sessionProxy.reject();
            }
        }
    }
}
