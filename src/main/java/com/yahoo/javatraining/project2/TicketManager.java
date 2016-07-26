package com.yahoo.javatraining.project2;

import com.yahoo.javatraining.project2.util.Storage;
import com.yahoo.javatraining.project2.util.WebService;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The ticket manager is used to manage the purchase of tickets.
 * This library assumes that the state of the tickets is stored in a storage system
 * and that tickets are purchased via a webservice call.
 * Furthermore, the ticket manager does not assume that the storage system is thread-safe;
 * it will carefully synchronize all calls to the storage system.
 * Also, the ticket manager will make concurrent calls to the webservice, in order to
 * minimize latency.
 */
public class TicketManager {

    private boolean available;
    private boolean isSoldOut;
    private long expiryInterval;

    private ConcurrentHashMap<String, Ticket> ticketMap;
    private CopyOnWriteArrayList<Ticket> ticketList;
    private CopyOnWriteArrayList<String> ticketLockList = new CopyOnWriteArrayList<String>();
    private Storage storage;
    private WebService webservice;

    private Lock lock = new ReentrantLock();
    private Lock storageLock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    //private ExecutorService multithreadService = Executors.newFixedThreadPool(50);
    //private ExecutorService multithreadService = new ThreadPoolExecutor(50, 50, 0L, TimeUnit.SECONDS, new PriorityBlockingQueue());
    private ExecutorService multithreadService;

    private ScheduledExecutorService expirationCheckService = Executors.newScheduledThreadPool(1);
    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * Constructs a ticket manager
     *
     * @param storage    A storage instance for storing updates to the tickets.
     * @param webservice A service instance to use for purchases.
     */
    public TicketManager(final long expireTimeMs, @NotNull Storage storage, @NotNull WebService webservice)
            throws TicketManagerException {
        this.available = true;
        this.isSoldOut = true;
        this.expiryInterval = expireTimeMs;
        //this.expiryInterval = 100;
        this.storage = storage;
        this.webservice = webservice;

        ticketMap = new ConcurrentHashMap<String, Ticket>();
        ticketList = new CopyOnWriteArrayList<Ticket>();
        for (Ticket ticket : getStorage().getTickets()) {
            ticketMap.put(ticket.getId(), ticket);
            ticketList.add(ticket);

            if (ticket.getStatus() != TicketStatusCode.BOUGHT) {
                isSoldOut = false;
            }
        }
        available = true;

        /*
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
        final long expiryInterval = expireTimeMs;
        final ScheduledFuture<?> future = service.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    System.out.println(tickets().size());
                    for (Ticket ticket : tickets()) {
                        if (ticket.getStatus() == TicketStatusCode.HELD && (System.currentTimeMillis() - ticket.getHoldTime()) > expiryInterval) {
                            System.out.println(ticket.getId() + " is expired");
                            cancel(null, ticket.getId(), null);
                        }
                    }
                } catch (TicketManagerException e) {
                    e.printStackTrace();
                }
            }
        }, 0, expireTimeMs, TimeUnit.MILLISECONDS);

        service.schedule(new Runnable() {
            public void run() {
                future.cancel(true);
            }
        }, 60 * 60, TimeUnit.SECONDS);
        */

        if (ticketList.parallelStream().filter(t -> (t.getStatus() == TicketStatusCode.BUYING || t.getStatus() == TicketStatusCode.BOUGHT)).count() == ticketList.size()) {
            // calling shutdown() will cause the crash
            //expirationCheckService.shutdown();
            //scheduledExecutorService.shutdown();

            multithreadService = Executors.newFixedThreadPool(50, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "sean's thread");
                }
            });

            ticketList.parallelStream().filter(t -> t.getStatus() == TicketStatusCode.BUYING).forEach(t -> {
                multithreadService.execute(() -> {
                    while (Thread.currentThread().getName().startsWith("sean")) {
                        try {
                            buyThroughService(t.getUserId(), t.getId(), t.getHoldTransId());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (TicketManagerException e) {
                            e.printStackTrace();
                        }
                    }
                });
            });
        }

        expirationCheckService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                for (Ticket ticket : ticketList) {
                    ticketLockList.remove(ticket.getId());
                    if (ticket.getStatus() == TicketStatusCode.HELD && (System.currentTimeMillis() - ticket.getHoldTime()) > expireTimeMs) {
                        try {
                            //cancel(null, ticket.getId(), null);
                            cancelWithoutCheck(ticket.getId());
                        } catch (TicketManagerException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }, 0, expireTimeMs, TimeUnit.SECONDS);
    }

    public Storage getStorage() {
        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        lock.lock();
        try {
            while (available == false) {
                try {
                    condition.await();
                } catch (InterruptedException e) { }
            }
            available = false;
            condition.signalAll();

        } finally {
            lock.unlock();
            return storage;
        }
    }

    /**
     * Rejects further calls to this class and shutdowns on-going concurrent tasks.
     * The object is no longer usable after this call.
     *
     * @throws InterruptedException If the shutdown was interrupted.
     */
    public void shutdown() throws InterruptedException {
        expirationCheckService.shutdown();
        if (multithreadService != null) {
            multithreadService.shutdown();
        }
        scheduledExecutorService.shutdown();

        ticketList.stream().forEach(t -> {
            try {
                getStorage().update(t);
                available = true;
            } catch (TicketManagerException e) {
                e.printStackTrace();
            }
        });
    }

    public List<Ticket> tickets() {
        //return ticketList.parallelStream().filter(t -> t.getStatus() == TicketStatusCode.AVAILABLE).collect(Collectors.toList());
        return ticketList;
    }

    /*
     * Returns the number of available tickets that are in the AVAILABLE and HELD states.
     * If greater than 0, it means that the tickets have not been sold out yet.
     * This method is thread-safe.
     *
     * @return Count of available tickets.
     */
    public int availableCount() {
        /*
        int count = 0;

        for (Ticket ticket : ticketList) {
            //System.out.println(ticket.getId() + " " + ticket.getStatus());
            if (ticket.getStatus() == TicketStatusCode.AVAILABLE || ticket.getStatus() == TicketStatusCode.HELD) {
                count++;
            }
        }

        return count;
        */

        return (int)ticketList.parallelStream().filter(t -> (t.getStatus() == TicketStatusCode.AVAILABLE || t.getStatus() == TicketStatusCode.HELD)).count();
    }

    /**
     * Holds the ticket. More specifically, sets the status to be HELD, generates a hold transaction id, and sets
     * the hold time. This method is thread-safe.
     * <p>
     * The hold trans id is returned if the ticket is already being held.
     *
     * @param userId   A user id.
     * @param ticketId A ticket id.
     * @return transaction id A transaction id.
     * @throws IllegalStateException Is thrown if the hold fails.
     */
    public
    @NotNull
    String hold(@NotNull String userId, @NotNull String ticketId) throws TicketManagerException {
        Ticket ticket = getTicket(ticketId);

        if (ticket.getUserId() != null && !ticket.getUserId().equals(userId)) {
            ticketLockList.remove(ticketId);
            throw new TicketManagerException(ticket.getId() + " is already held");
        }

        ticket.setHoldTime(System.currentTimeMillis());
        String holdTransId = UUID.randomUUID().toString();
        ticket.setHoldTransId(holdTransId);
        ticket.setStatus(TicketStatusCode.HELD);
        ticket.setUserId(userId);

        getStorage().update(ticket);
        available = true;
        ticketLockList.remove(ticketId);

        final String currentTicketId = ticketId;
        scheduledExecutorService.schedule(new Runnable() {
            public void run() {
                try {
                    Ticket currentTicket = getTicket(currentTicketId);
                    ticketLockList.remove(currentTicketId);
                    if (currentTicket.getStatus() == TicketStatusCode.HELD) {
                        System.out.println(currentTicket.getId() + " is expired");
                        cancelWithoutCheck(currentTicket.getId());
                    }
                } catch (TicketManagerException e) {
                    e.printStackTrace();
                }
            }
        }, expiryInterval, TimeUnit.MILLISECONDS);

        return holdTransId;
    }

    /**
     * Cancels a held ticket. The ticket's state becomes AVAILABLE, the hold transaction id is cleared, and the
     * hold time is cleared. The userId and holdTransId must match the persisted values or the cancel will fail.
     * This method is thread-safe.
     * <p>
     * Returns false if the ticket has already been cancelled.
     *
     * @param userId      A user id.
     * @param ticketId    A ticket id.
     * @param holdTransId A hold transaction id.
     * @return true If the cancel succeeded.
     * @throws IllegalStateException Is thrown if the cancel fails.
     */
    public boolean cancel(@NotNull String userId, @NotNull String ticketId, @NotNull String holdTransId) throws TicketManagerException {
        Ticket ticket = getTicket(ticketId);

        if ((ticket.getUserId() != null && !ticket.getUserId().equals(userId)) || (ticket.getHoldTransId() != null && !ticket.getHoldTransId().equals(holdTransId))) {
            ticketLockList.remove(ticketId);
            throw new TicketManagerException("cancel error");
        }

        ticketLockList.remove(ticketId);
        cancelWithoutCheck(ticketId);

        return false;
    }

    private boolean cancelWithoutCheck(@NotNull String ticketId) throws TicketManagerException {
        Ticket ticket = getTicket(ticketId);

        ticket.setBuyingTime(0);
        ticket.setBuyTransId(null);
        ticket.setHoldTime(0);
        ticket.setHoldTransId(null);
        ticket.setStatus(TicketStatusCode.AVAILABLE);
        ticket.setUserId(null);

        getStorage().update(ticket);
        available = true;
        ticketLockList.remove(ticketId);

        return false;
    }

    /**
     * Buys a held ticket. The ticket's state becomes BOUGHT and the buy transaction id is set.
     * The userId and holdTransId must match the persisted values or the buy will fail.
     * This method is thread-safe.
     *
     * @param userId      A user id.
     * @param ticketId    A ticket id.
     * @param holdTransId A hold transaction id.
     * @return The buy transaction id.
     * @throws IllegalStateException Is thrown if the buy fails.
     */
    public
    @NotNull
    String buy(@NotNull String userId, @NotNull String ticketId, @NotNull String holdTransId)
            throws TicketManagerException, InterruptedException {
        Ticket ticket = getTicket(ticketId);

        if (!userId.equals(ticket.getUserId()) || !holdTransId.equals(ticket.getHoldTransId())) {
            ticketLockList.remove(ticketId);
            throw new TicketManagerException("buy error");
        }

        String buyTransId = "*";
        if (ticket.getStatus() == TicketStatusCode.HELD) {
            ticket.setBuyingTime(System.currentTimeMillis());
            ticket.setBuyTransId(buyTransId);
            ticket.setStatus(TicketStatusCode.BUYING);

            getStorage().update(ticket);
            available = true;
        }
        ticketLockList.remove(ticketId);

        return buyTransId;
    }

    private String buyThroughService(@NotNull String userId, @NotNull String ticketId, @NotNull String holdTransId)
            throws TicketManagerException, InterruptedException {
        Ticket ticket = ticketMap.get(ticketId);

        String buyTransId = null;
        if (ticket.getStatus() == TicketStatusCode.BUYING) {
            try {
                buyTransId = webservice.buy(ticketId, userId);

                ticket.setBuyTransId(buyTransId);
                ticket.setStatus(TicketStatusCode.BOUGHT);

                getStorage().update(ticket);
                available = true;

            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        }

        return buyTransId;
    }

    /**
     * Blocks until all tickets are in the BOUGHT state.
     * The caller should not shutdown this instance until this method returns.
     * This method is thread-safe.
     *
     * @throws InterruptedException If the thread is interrupted.
     */
    public void awaitAllBought() throws InterruptedException {
        lock.lock();
        try {
            while (isSoldOut == false) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            condition.signalAll();

        } finally {
            lock.unlock();
        }
    }

    public Ticket getTicket(String ticketId) {
        if (!ticketLockList.contains(ticketId)) {
            ticketLockList.add(ticketId);
            return ticketMap.get(ticketId);
        }

        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        lock.lock();
        try {
            while (ticketLockList.contains(ticketId)) {
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            ticketLockList.add(ticketId);
            condition.signalAll();

        } finally {
            lock.unlock();
            return ticketMap.get(ticketId);
        }
    }
}