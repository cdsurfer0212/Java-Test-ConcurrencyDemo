package com.yahoo.javatraining.project2.util;

import com.yahoo.javatraining.project2.Ticket;
import com.yahoo.javatraining.project2.TicketManagerException;
import com.yahoo.javatraining.project2.TicketStatusCode;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * This storage system keeps the persistent state in a file.
 * The ticket state in the file are stored one per line. There are four possible formats:
 *
 * 1. ticketId - is a ticket that is availableCount for purchase
 * 2. ticketId userId holdTransactionId - is a ticket that's currently being held by userId
 * 3. ticketId userId holdTransactionId * - is a ticket that's in the process of being purchased
 * 4. ticketId userId holdTransactionId buyTransactionId - is a purchased ticket
 */
public class Storage {
    File file;

    /**
     * Manages the tickets in a file.
     *
     * @param file Non-null file of tickets. The format is described in the class docs.
     */
    public Storage(@NotNull File file) {
        this.file = file;
    }

    /**
     * Returns the list of tickets currently stored in the file.
     * This method is not thread-safe.
     *
     * @return List of Ticket objects.
     * @throws TicketManagerException If the retrieval of the tickets did not succeed.
     */
    public @NotNull List<Ticket> getTickets() throws TicketManagerException {
        List<Ticket> results = new ArrayList<>();
        try (Scanner sc = new Scanner(file)) {
            while (sc.hasNext()) {
                results.add(deserialize(sc.nextLine()));
            }
        } catch (FileNotFoundException e) {
            throw new TicketManagerException(e);
        }
        return results;
    }

    /**
     * Updates the storage with the state of the supplied Ticket object.
     * This method is not thread-safe.
     *
     * @param ticket A ticket instance.
     * @throws TicketManagerException If the update did not succeed.
     */
    public void update(@NotNull Ticket ticket) throws TicketManagerException {
        File newFile = new File(file.toString() + ".new");
        Scanner sc = null;
        try {
            sc = new Scanner(file);
            FileWriter wr = new FileWriter(newFile);
            try {
                while (sc.hasNext()) {
                    String line = sc.nextLine();
                    String[] parts = line.split(" ");
                    if (parts[0].equals(ticket.getId())) {
                        wr.write(serialize(ticket));
                    } else {
                        wr.write(line);
                    }
                    wr.write('\n');
                }
            } finally {
                if (sc != null) {
                    sc.close();
                }
                if (wr != null) {
                    wr.close();
                    newFile.renameTo(file);
                }
            }
        } catch (IOException e) {
            throw new TicketManagerException(e);
        }
    }

    /**
     * Returns a ticket object given the formatted line.
     * @param line A line from a ticket file.
     * @return A ticket instance.
     */
    private @NotNull Ticket deserialize(@NotNull String line) {
        Ticket ticket = null;
        String[] parts = line.split(" ");
        if (parts.length == 0 || parts.length == 2 || parts.length > 5) {
            throw new IllegalStateException("Invalid line: " + line);
        }
        if (parts.length >= 1) {
            ticket = new Ticket(parts[0]);
        }
        if (parts.length >= 3) {
            ticket.setStatus(TicketStatusCode.HELD);
            ticket.setUserId(parts[1]);
            ticket.setHoldTransId(parts[2]);
        }
        if (parts.length >= 4) {
            if (parts[3].equals("*")) {
                ticket.setStatus(TicketStatusCode.BUYING);
            } else {
                ticket.setStatus(TicketStatusCode.BOUGHT);
                ticket.setBuyTransId(parts[3]);
            }
        }
        return ticket;
    }

    /**
     * Converts the state in a Ticket object into a string.
     * @param ticket A ticket instance.
     * @return A string representing the supplied ticket.
     */
    private @NotNull String serialize(@NotNull Ticket ticket) {
        switch (ticket.getStatus()) {
            case AVAILABLE:
                return String.format("%s", ticket.getId());
            case HELD:
                return String.format("%s %s %s", ticket.getId(), ticket.getUserId(), ticket.getHoldTransId());
            case BUYING:
                return String.format("%s %s %s *", ticket.getId(), ticket.getUserId(), ticket.getHoldTransId());
            case BOUGHT:
                return String.format("%s %s %s %s", ticket.getId(), ticket.getUserId(), ticket.getHoldTransId(),
                        ticket.getBuyTransId());
        }
        throw new IllegalStateException("Unknown state: " + ticket.getStatus());
    }
}
