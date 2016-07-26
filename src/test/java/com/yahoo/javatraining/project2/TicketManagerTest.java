package com.yahoo.javatraining.project2;

import com.yahoo.javatraining.project2.util.Storage;
import com.yahoo.javatraining.project2.util.WebService;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 */
public class TicketManagerTest {
    File file = new File("/tmp/tickets");
    TicketManager tmgr;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        WebService.randomFailures = false;

        // Generate file of tickets
        try (FileWriter wr = new FileWriter(file)) {
            for (int i = 0; i < 10; i++) {
                wr.write(i + "\n");
            }
        }
        tmgr = new TicketManager(100, new Storage(file), new WebService());
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        tmgr.shutdown();
    }

    @Test
    public void hold() throws Exception {
        String txId = tmgr.hold("user", "0");
        Assert.assertEquals(getLine(0), "0 user " + txId);
    }

    @Test
    public void alreadyHeld() throws Exception {
        try {
            tmgr.hold("user1", "0");
            tmgr.hold("user1", "0");
        } catch (TicketManagerException e) {
            Assert.fail("unexpected exception", e);
        }
    }

    @Test
    public void heldByAnother() throws Exception {
        try {
            tmgr.hold("user1", "0");
            tmgr.hold("user2", "0");
            Assert.fail("expected exception");
        } catch (TicketManagerException e) {
        }
    }

    @Test
    public void cancel() throws Exception {
        String txId = tmgr.hold("user", "3");
        tmgr.cancel("user", "3", txId);
        Assert.assertEquals(getLine(3), "3");
    }

    @Test
    public void cancelWithoutHold() throws Exception {
        try {
            tmgr.cancel("user1", "1", "junk");
        } catch (TicketManagerException e) {
            Assert.fail("unexpected exception", e);
        }
    }

    @Test
    public void cancelWithWrongHoldTxId() throws Exception {
        try {
            tmgr.hold("user1", "3");
            tmgr.cancel("user1", "3", "junk");
            Assert.fail("expected exception");
        } catch (TicketManagerException e) {
        }
    }

    @Test
    public void buy() throws Exception {
        String holdTxId = tmgr.hold("user", "5");
        String buyTxId = tmgr.buy("user", "5", holdTxId);
        Assert.assertEquals(getLine(5), "5 user " + holdTxId + " " + buyTxId);
    }

    @Test
    public void buyWithoutHold() throws Exception {
        try {
            tmgr.buy("user1", "7", "junk");
            Assert.fail("expected exception");
        } catch (TicketManagerException e) {
        }
    }

    @Test
    public void buyWithWrongHoldTxId() throws Exception {
        try {
            tmgr.hold("user", "8");
            tmgr.buy("user1", "8", "junk");
            Assert.fail("expected exception");
        } catch (TicketManagerException e) {
        }
    }

    @Test
    public void expire() throws Exception {
        tmgr.hold("user", "4");
        Thread.sleep(200);
        Assert.assertEquals(getLine(4), "4");
    }

    public String getLine(int n) throws Exception {
        try (Scanner sc = new Scanner(file)) {
            for (int i = 0; i < n; i++) {
                sc.nextLine();
            }
            return sc.nextLine();
        }
    }
}
