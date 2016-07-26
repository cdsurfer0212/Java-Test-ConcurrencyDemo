package com.yahoo.javatraining.project2;

import javax.validation.constraints.NotNull;

/**
 * This is the main exception thrown by the TicketManager library.
 */
public class TicketManagerException extends Exception {
    /**
     * Constructs an exception with just message.
     * @param message An exception messsage.
     */
    public TicketManagerException(@NotNull String message) {
        super(message);
    }

    /**
     * Wraps another exception.
     * @param cause An exception.
     */
    public TicketManagerException(@NotNull Throwable cause) {
        super(cause);
    }

    /**
     * Wraps another exception with a more detailed exception message.
     * @param message An exception message.
     * @param cause An exception.
     */
    public TicketManagerException(@NotNull String message, @NotNull Throwable cause) {
        super(message, cause);
    }
}
