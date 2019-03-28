package org.openpaas.servicebroker.postgresql.exception;

import org.openpaas.servicebroker.exception.ServiceBrokerException;

public class PostgresqlServiceException extends ServiceBrokerException {

	private static final long serialVersionUID = 5892451725171626541L;

	public PostgresqlServiceException(String message) {
		super(message);
	}
	
}
