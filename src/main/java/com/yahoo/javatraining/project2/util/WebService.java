package com.yahoo.javatraining.project2.util;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;

/**
 * This is a client library that accesses a webservice for purchasing tickets.
 */
public class WebService {

    // If true, the buy method will randomly throw exceptions
    public static boolean randomFailures = true;

    /**
     * This method is called to purchase a ticket. It will block until the purchase is confirmed.
     * If the ticket was already purchased by the user, the previous buy transaction id is returned.
     * This method is thread-safe; it can be called by up to 5 concurrent threads.
     *
     * @param ticketId A ticket id.
     * @param userId A user id.
     * @return A buy transaction id.
     */
    public @NotNull String buy(@NotNull String ticketId, @NotNull String userId) {
        try {
            Thread.sleep((int)(Math.random() * 500)); // artificial delay
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (randomFailures && Math.random() < .01) {
            throw new IllegalStateException("Random exception");
        }

        return ticketId + "-X";
    }
}
