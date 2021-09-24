package edu.uci.ics.texera.web.auth;

import io.dropwizard.auth.Authorizer;

public class UserRoleAuthorizer implements Authorizer<SessionUser> {

	@Override
	public boolean authorize(SessionUser user, String role) {

		// currently we do not have multiple roles, grant any SessionUser.
		return true;
	}
}
