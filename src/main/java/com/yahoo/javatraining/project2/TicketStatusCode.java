package com.yahoo.javatraining.project2;

/**
 * This enumeration represents the four possible ticket states.
 *
 * Implementation Note: The reason the purchased state is split into two states (BUYING and BOUGHT)
 * is to provide proper recovery after a server crash.
 * In particular, the intent to purchase is first persisted before attempting to call the webservice.
 * If the server crashes before the webservice call is complete, the next time the server starts up, it
 * can call the webservice again, since the call is idempotent. It's also important not to call the webservice
 * until the ticket status is persisted because if the webservice call succeeds before the state is persisted and
 * the server crashes, the sold ticket will be on sale again when the server starts up.
 */
public enum TicketStatusCode {
    /**
     * Indicates that the ticket is available for hold. The only valid next state
     * is HELD.
     */
    AVAILABLE,

    /**
     * Indicates that the ticket is being held by a user. The two valid next states
     * are AVAILABLE (if the ticket is cancelled) or BUYING (if the ticket is being bought).
     */
    HELD,

    /**
     * Indicates that the ticket is being bought. More specifically, the ticket must
     * first be persisted in the BUYING state successfully before attempting to call the webservice
     * to purchase the ticket.
     */
    BUYING,

    /**
     * Indicates that the ticket was successfully purchased. More specifically, the webservice
     * call to purchase the ticket succeeded.
     */
    BOUGHT
}
