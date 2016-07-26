package com.yahoo.javatraining.project2;

import com.yahoo.javatraining.project2.util.Storage;
import com.yahoo.javatraining.project2.util.WebService;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class ConcurrentUserTester {
    private static final int NUM_USERS = 50;
    private static final int NUM_TICKETS = 500;

    public static void main(String[] args) throws Exception {
        File file = new File("/tmp/tickets");

        // Generate file of tickets
        if (args.length < 1 || !args[0].equals("-restart")) {
            try (FileWriter wr = new FileWriter(file)) {
                for (int i = 1; i <= NUM_TICKETS; i++) {
                    wr.write(i + "\n");
                }
            }
        }

        // Start up ticket manager
        TicketManager tmgr = new TicketManager(10, new Storage(file), new WebService());

        // Start up many concurrent users
        List<Ticket> tickets = tmgr.tickets();
        List<Future<?>> users = new ArrayList<>();
        ExecutorService userService = Executors.newFixedThreadPool(NUM_USERS);
        AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < NUM_USERS; i++) {
            users.add(userService.submit(new User("user-" + i, tmgr, stop)));
        }

        // Simulate a crash after 5 seconds
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(5000);
                    System.out.println("****************************************** CRASH ****************************************");
                    Runtime.getRuntime().halt(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        // Wait until there are no more unbought tickets
        tmgr.awaitAllBought();

        // Stop all the users
        stop.set(true);
        for (Future<?> u : users) {
            u.get();
        }
        tmgr.shutdown();

        // Verify that all tickets have been purchased
        tickets = new Storage(file).getTickets();
        int errors = 0;
        for (Ticket t : tickets) {
            if (t.getStatus() != TicketStatusCode.BOUGHT) {
                if (errors == 0) {
                    System.out.println("****************************************** ERROR ****************************************");
                    System.out.println("TicketManager.awaitAllBought() claims WebService.buy() has succeeded for all tickets.");
                    System.out.println("However, the following ticket status are not in BOUGHT status:");
                }
                System.out.printf("  Ticket %s: %s\n", t.getId(), t.getStatus());
                errors++;
            }
        }
        System.exit(0);
    }
}

class User implements Runnable {
    private final String userId;
    private final TicketManager tmgr;
    private final AtomicBoolean stop;
    private final List<Ticket> tickets;

    User(String userId, TicketManager tmgr, AtomicBoolean stop) {
        this.userId = userId;
        this.tmgr = tmgr;
        this.tickets = tmgr.tickets();
        this.stop = stop;
    }

    public void run() {
        while (!stop.get() && tmgr.availableCount() > 0) {
            int r = rand(tickets.size());
            Ticket ticket = null;
            for (int i = 0; i < tickets.size(); i++) {
                ticket = tickets.get((i + r) % tickets.size());
                if (ticket.getStatus() == TicketStatusCode.AVAILABLE) {
                    break;
                }
                continue;
            }
            try {
                switch (rand(2)) {
                    case 0:
                        // Hold and cancel
                        String txId = tmgr.hold(userId, ticket.getId());
                        Thread.sleep(rand(50)); // possibly expire

                        tmgr.cancel(userId, ticket.getId(), txId);
                        System.out.printf("%s cancelled ticket %s\n", userId, ticket.getId());
                        break;
                    case 1:
                        // Buy
                        txId = tmgr.hold(userId, ticket.getId());
                        Thread.sleep(rand(50)); // possibly expire

                        tmgr.buy(userId, ticket.getId(), txId);
                        System.out.printf("%s completed purchase of ticket %s. %d tickets left\n",
                                userId, ticket.getId(), tmgr.availableCount());
                        break;
                }
            } catch (TicketManagerException e) {
                if (e.getMessage() == null
                        || !e.getMessage().matches(".*(is|being held|is being or has been purchased|first be held|no longer available|already held).*")) {
                    e.printStackTrace();
                } else {
                    System.out.printf("%s %s. %d tickets left\n",
                            userId, e.getMessage(), tmgr.availableCount());
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private int rand(int n) {
        return (int) (Math.random() * n);
    }
}
