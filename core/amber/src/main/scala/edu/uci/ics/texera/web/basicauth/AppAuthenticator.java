package edu.uci.ics.texera.web.basicauth;

import edu.uci.ics.texera.web.SqlServer;
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User;
import io.dropwizard.auth.Authenticator;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.JwtContext;

import java.util.Optional;

import static edu.uci.ics.texera.web.model.jooq.generated.Tables.USER;

public class AppAuthenticator implements Authenticator<JwtContext, SessionUser> {

    @Override
    public Optional<SessionUser> authenticate(JwtContext context) {
        // This method will be called once the token's signature has been verified

        try {
            // TODO: verify that the provided token has not expired.
            final String userName = context.getJwtClaims().getSubject();
            Optional<User> user = Optional.ofNullable(SqlServer.createDSLContext()
                    .select()
                    .from(USER)
                    .where(USER.NAME.eq(userName).and(USER.GOOGLE_ID.isNull()))
                    .fetchOneInto(User.class));

            return user.map(SessionUser::new);

        } catch (MalformedClaimException e) {
            return Optional.empty();
        }
        // All JsonWebTokenExceptions will result in a 401 Unauthorized response.
    }
}
