package edu.uci.ics.texera.web.auth;

import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User;

import java.security.Principal;

public class SessionUser implements Principal {

    private final User user;

    public SessionUser(User user) {
        this.user = user;
    }


    public User getUser() {
        return user;
    }

    @Override
    public String getName() {
        return user.getName();
    }
}
