/*
 * Copyright 2010-2012 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.List;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

import org.junit.*;

import com.amazon.carbonado.repo.dirmi.*;

import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Session;

import com.amazon.carbonado.*;

import com.amazon.carbonado.capability.RemoteProcedure;
import com.amazon.carbonado.capability.RemoteProcedureCapability;
import com.amazon.carbonado.capability.ResyncCapability;

import com.amazon.carbonado.repo.map.MapRepositoryBuilder;
import com.amazon.carbonado.repo.replicated.ReplicatedRepositoryBuilder;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueProducer;

import com.amazon.carbonado.synthetic.SyntheticStorableBuilder;

import com.amazon.carbonado.stored.StorableTestVersioned;

/**
 * 
 *
 * @author Olga Kuznetsova
 * @author Brian S O'Neill
 */
public class RemoteTest {
    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main(RemoteTest.class.getName());
    }

    @Test
    public void remoteBasicTest() throws Exception {
        Session[] sessionPair = new Environment().newSessionPair();
        sessionPair[0].send("hi");
        assertEquals("hi", sessionPair[1].receive());
    }

    @Test
    public void mapRepoInsertTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();

        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();

        assertTrue(!(remoteRepo instanceof RemoteRepositoryServer));

        Repository clientRepo = ClientRepository.from(remoteRepo);
        assertTrue(!(clientRepo instanceof RemoteRepository));
        assertTrue(clientRepo instanceof ClientRepository);

        Storage<StorableTestVersioned> storage = repo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb = storage.prepare();
        stb.setId(2);
        if (!stb.tryLoad()) {
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.tryInsert();
        }

        Storage <StorableTestVersioned> clientstorage =
            clientRepo.storageFor(StorableTestVersioned.class);
        assertTrue(!(repo.equals(clientRepo)));
        StorableTestVersioned stb1 = clientstorage.prepare();
        stb1.setId(2);
        assertTrue(stb1.tryLoad());
        assertEquals("world", stb1.getStringProp());
        assertEquals(321, stb1.getIntProp());
        assertEquals(313244232323432L, stb1.getLongProp());

        /////////////////////////
        stb = storage.prepare();
        stb.setId(3);
        if (!stb.tryLoad()) {
            stb.setStringProp("hello");
            stb.setIntProp(1);
            stb.setLongProp(3L);
            stb.setDoubleProp(1.4);
            stb.tryInsert();
        }   

        stb1 = clientstorage.prepare();
        stb1.setId(3);
        assertTrue(stb1.tryLoad());
        assertEquals("hello", stb1.getStringProp());

        ///////////////////////
        stb1 = clientstorage.prepare();
        stb1.setId(2);
        assertTrue(stb1.tryLoad());
        stb1.setStringProp("new world");
        assertTrue(stb1.tryUpdate());

        stb = storage.prepare();
        stb.setId(2);
        assertTrue(stb.tryLoad());
        assertEquals("new world", stb.getStringProp());
    }

    @Test
    public void sequenceTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();

        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();

        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<SeqRec> clientStorage = clientRepo.storageFor(SeqRec.class);
        SeqRec rec = clientStorage.prepare();
        rec.setValue("value");
        rec.insert();

        assertEquals(1, rec.getId());

        // Allow sequence to be bypassed.
        rec = clientStorage.prepare();
        rec.setValue("value2");
        rec.setId(100);
        rec.insert();

        assertEquals(100, rec.getId());
        rec.load();
        assertEquals("value2", rec.getValue());
    }

    @PrimaryKey("id")
    public static interface SeqRec extends Storable {
        @Sequence("ID")
        int getId();
        void setId(int id);

        String getValue();
        void setValue(String value);
    }

    @Test
    public void transactionsTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        final Repository clientRepo = ClientRepository.from(remoteRepo);

        final Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb = clientStorage.prepare();
        stb.setId(2);
        stb.setStringProp("world");
        stb.setIntProp(321);
        stb.setLongProp(313244232323432L);
        stb.setDoubleProp(1.423423);
        stb.insert();
        Transaction txn = clientRepo.enterTransaction();
        try {
            stb.setStringProp("world1");
            stb.update();
            Thread otherThread = new Thread() {
                    public void run() {
                        Transaction txn1 = 
                            clientRepo.enterTransaction(IsolationLevel.READ_COMMITTED);
                        try {
                            StorableTestVersioned b = clientStorage.prepare();
                            b.setId(2);
                            try {
                                isSet = b.tryLoad();
                                newTransactionValue = b.getStringProp();
                            } finally {
                                txn1.exit();
                            }
                        } catch (Exception e) {
                            isSet = false;
                        }
                    }
                };
            otherThread.start();
            otherThread.join();
            // assertEquals(true, isSet);
            // assertEquals("world1", newTransactionValue);

            txn.commit();
        } finally {
            txn.exit();
        }
    }

    private volatile boolean isSet = true;
    private volatile String newTransactionValue;

    @Test
    public void transactionsAttachDetachTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        final Repository clientRepo = ClientRepository.from(remoteRepo);

        final Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb = clientStorage.prepare();
        stb.setId(2);
        stb.setStringProp("world");
        stb.setIntProp(321);
        stb.setLongProp(313244232323432L);
        stb.setDoubleProp(1.423423);
        stb.insert();
        final Transaction txn = clientRepo.enterTransaction();
        try {
            Query<StorableTestVersioned> query = clientStorage.query();
            Cursor<StorableTestVersioned> cursor = query.fetch();
            try {
                final Transaction txn1 = clientRepo.enterTransaction();
                Thread otherThread = new Thread() {
                    public void run() {
                        txn.attach();
                    }
                };

                otherThread.start();
                otherThread.join();
            } catch (Exception e) {
                assertTrue(false);
            }
        } finally {
            txn.exit();
        }

    }

    @Test
    public void queryTest() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb = clientStorage.prepare();
        stb.setId(2);
        if (!stb.tryLoad()) {
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.tryInsert();
        }
    }

    @Test
    public void queryTest2() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        for (int i=0; i<299; i++) {
            StorableTestVersioned stb = clientStorage.prepare();
            stb.setId(i);
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

        assertEquals(299, clientStorage.query().fetch().toList().size());
    }

    @Test
    public void queryTest3() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        Transaction txn = clientRepo.enterTransaction();
        try {
            for (int i=0; i<299; i++) {
                StorableTestVersioned stb = clientStorage.prepare();
                stb.setId(i);
                stb.setStringProp("world");
                stb.setIntProp(321);
                stb.setLongProp(313244232323432L);
                stb.setDoubleProp(1.423423);
                stb.insert();
            }

            assertEquals(299, clientStorage.query().fetch().toList().size());
        } finally {
            txn.exit();
        }
    }

    @Test
    public void queryTest4() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        for (int i=0; i<1000; i++) {
            StorableTestVersioned stb = clientStorage.prepare();
            stb.setId(i);
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

        Transaction txn = clientRepo.enterTransaction();
        try {
            // Acquire write locks to prevent access by other transactions.
            txn.setForUpdate(true);

            Cursor<StorableTestVersioned> c = clientStorage.query().fetch();

            // Get the server fetch started and blocked waiting for us to drain the pipe.
            c.hasNext();
            Thread.sleep(1000);

            // There should be no problem in reading same records because we're in the same
            // transaction.
            {
                Cursor<StorableTestVersioned> c2 = clientStorage.query().fetch();
                int count = 0;
                while (c2.hasNext()) {
                    StorableTestVersioned stb = c2.next();
                    assertEquals(count, stb.getId());
                    count++;
                }
            }

            // Finish the original cursor.
            int count = 0;
            while (c.hasNext()) {
                StorableTestVersioned stb = c.next();
                assertEquals(count, stb.getId());
                count++;
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void queryTimeoutTest() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        for (int i=0; i<299; i++) {
            StorableTestVersioned stb = clientStorage.prepare();
            stb.setId(i);
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

        assertEquals(299, clientStorage.query().fetch(Query.Timeout.seconds(10)).toList().size());

        try {
            clientStorage.query().fetch(Query.Timeout.nanos(1)).toList();
            fail();
        } catch (FetchTimeoutException e) {
        }
    }

    @Test
    public void queryTimeoutTest2() throws Exception { 
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        for (int i=0; i<299; i++) {
            StorableTestVersioned stb = clientStorage.prepare();
            stb.setId(i);
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

        assertEquals(299, clientStorage.query().count());
        assertEquals(299, clientStorage.query("stringProp != ?").with("x").count());

        try {
            clientStorage.query("stringProp != ?").with("x").count(Query.Timeout.nanos(1));
            fail();
        } catch (FetchTimeoutException e) {
        }
    }

    @Test
    public void closingTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb = clientStorage.prepare();
        stb.setId(2);
        if (!stb.tryLoad()) {
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.tryInsert();
        }

        clientRepo.close();

        Storage<StorableTestVersioned> actualStorage =
            repo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb1 = actualStorage.prepare();
        stb1.setId(3);
        boolean caught = false;
        try {
            stb.tryLoad();
        } catch (IllegalStateException e) {
            caught = true;
        }
        assertTrue(caught);
    }

    @Test
    public void reconnectStressTest() throws Exception {
        Set<Repository> repos = new HashSet<Repository>();
        for (int i = 0; i < 10; ++i) {
            Repository repo = MapRepositoryBuilder.newRepository();
            repos.add(repo);
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            Repository clientRepo = ClientRepository.from(remoteRepo);
            Storage<StorableTestVersioned> storage =
                clientRepo.storageFor(StorableTestVersioned.class);
            StorableTestVersioned stb;
            fill(storage);

            SequenceCapability capability = clientRepo.getCapability(SequenceCapability.class);
            assertNotNull(capability);
            SequenceValueProducer producer = capability.getSequenceValueProducer("random");
            assertEquals(1, producer.nextIntValue());

            Query<StorableTestVersioned> query = storage.query();
            Cursor<StorableTestVersioned> cursor = query.fetch();

            assertEquals(0, cursor.next().getId());

            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);

            Storage<StorableTestVersioned> storage1 =
                clientRepo.storageFor(StorableTestVersioned.class);;
            assertNotNull(storage1);
            stb = storage.prepare();
            stb.setId(1);

            assertTrue(stb.tryLoad());
            assertEquals("1world", stb.getStringProp());

            assertEquals(2, producer.nextIntValue());
        }
    }

    @Test
    public void reconnectTransactionUpdateTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            stb = storage.prepare();
            stb.setId(1);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.setVersion(3);
            try {
                stb.update();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }


    @Test
    public void reconnectTransactionInsertTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            stb = storage.prepare();
            stb.setId(44);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.setVersion(3);
            try {
                stb.insert();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionLoadTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            stb = storage.prepare();
            stb.setId(1);
            try {
                stb.load();
                fail();
            } catch (FetchException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionDeleteTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(1);
            stb.load();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            stb.setVersion(3);
            try {
                stb.delete();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionNestedTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(1);
            stb.load();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            stb.setVersion(3);
            Transaction internalTxn;

            internalTxn = clientRepo.enterTransaction();

            StorableTestVersioned stbInternal = storage.prepare();
            stbInternal.setId(2);
            stbInternal.setVersion(4);
            try {
                stbInternal.update();
                fail();
            } catch (Exception e) {
                // expected
            }
            try {
                internalTxn.commit();
                fail();
            } catch (Exception e) {
                // expected
            }
        } finally {
            try {
                txn.exit();
                fail();
            } catch (Exception e) {
                // expected
            }
        }
    }

    @Test
    public void reconnectTransactionQueryTest() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.query().fetch().toList();
                fail();
            } catch (FetchException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionQueryCount() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.query().count();
                fail();
            } catch (FetchException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionDeleteOne() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.query("id = ?").with(0).deleteOne();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionTryDeleteOne() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.query("id = ?").with(0).tryDeleteOne();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionDeleteAll() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.query().deleteAll();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void reconnectTransactionTruncate() throws Exception {
        Repository repo = MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        fill(storage);

        Transaction txn = clientRepo.enterTransaction();
        try {
            stb = storage.prepare();
            stb.setId(33);
            stb.setStringProp(33 + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            pair[0].close();
            Session[] pair1 = new Environment().newSessionPair();
            pair1[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo1 = (RemoteRepository) pair1[1].receive();
            ((ClientRepository)clientRepo).reconnect(remoteRepo1);
            try {
                storage.truncate();
                fail();
            } catch (PersistException e) {
                //expected
            }
            try {
                txn.commit();
                fail();
            } catch (PersistException e) {
                //expected
            }
        } finally {
            txn.exit();
        }
    }

    @Test
    public void basicProcedureCall() throws Exception {
        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        try {
            cap.beginCall(null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        EchoProc proc = new EchoProc();

        for (int i=1; i<=2; i++) {
            proc.resetEchoCounts();

            List<String> reply = cap.beginCall(proc).send("hello").fetchReply().toList();
            assertEquals(1, reply.size());
            assertEquals("hello!", reply.get(0));
            if (i == 2) {
                proc.assertEchoCounts(1, 1);
            }

            // Again, sending too much. This is an exception.
            try {
                reply = cap.beginCall(proc).send("hello2").send("again").fetchReply().toList();
                fail();
            } catch (IllegalStateException e) {
                // Good.
            }
            if (i == 2) {
                proc.assertEchoCounts(2, 1);
            }

            // Sending too little.
            reply = cap.beginCall(proc).fetchReply().toList();
            assertTrue(reply.isEmpty());
            if (i == 2) {
                proc.assertEchoCounts(3, 2);
            }

            // Dropping reply.
            cap.beginCall(proc).send("hello").execute();
            if (i == 2) {
                proc.assertEchoCounts(4, 3);
            }
        }
    }

    @Test
    public void transactionProcedureCall() throws Exception {
        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        // First verify that insert works outside explicit transaction.
        cap.beginCall(new InsertProc(1)).execute();

        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);

        StorableTestVersioned stb = storage.prepare();
        stb.setId(1);
        stb.load();

        // This should throw an exception back.
        try {
            cap.beginCall(new InsertProc(1)).execute();
            fail();
        } catch (UniqueConstraintException e) {
            // Good.
        }

        Transaction txn = clientRepo.enterTransaction();
        try {
            cap.beginCall(new InsertProc(2)).execute();
            // Confirm visible within local transaction.
            stb = storage.prepare();
            stb.setId(2);
            stb.load();
            // Don't commit.
        } finally {
            txn.exit();
        }

        // Rolled back.
        stb = storage.prepare();
        stb.setId(2);
        assertFalse(stb.tryLoad());

        txn = clientRepo.enterTransaction();
        try {
            cap.beginCall(new InsertProc(2)).execute();
            // Confirm visible within local transaction.
            stb = storage.prepare();
            stb.setId(2);
            stb.load();
            txn.commit();
        } finally {
            txn.exit();
        }

        // Committed.
        stb = storage.prepare();
        stb.setId(2);
        stb.load();

        txn = clientRepo.enterTransaction();
        try {
            try {
                // Not allowed within a transaction.
                cap.beginCall(new InsertProc(3)).executeAsync();
                fail();
            } catch (IllegalStateException e) {
            }
        } finally {
            txn.exit();
        }

        // Allowed outside a transaction.
        cap.beginCall(new InsertProc(3)).executeAsync();
    }

    @Test
    public void remoteProcedureQuery() throws Exception {
        // Tests that Storables can be serialized from procedure to caller.

        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);

        fill(storage);

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        QueryProc proc = new QueryProc("id >= ? & id < ?", 5, 10);
        List<StorableTestVersioned> reply = cap.beginCall(proc).fetchReply().toList();

        assertEquals(5, reply.size());
        for (int i=0; i<5; i++) {
            assertEquals(i + 5, reply.get(i).getId());
        }
    }

    @Test
    public void remoteProcedureFill() throws Exception {
        // Tests that Storables can be serialized from caller to procedure.

        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        FillProc proc = new FillProc();
        RemoteProcedure.Call call = cap.beginCall(proc);
        for (int i=0; i<20; i++) {
            StorableTestVersioned stb = storage.prepare();
            stb.setId(i);
            stb.setStringProp("hello " + i);
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            call.send(stb);
        }
        call.execute();

        Cursor<StorableTestVersioned> c = storage.query().orderBy("id").fetch();
        int i = 0;
        while (c.hasNext()) {
            StorableTestVersioned stb = c.next();
            assertEquals(i, stb.getId());
            assertEquals("hello " + i, stb.getStringProp());
            i++;
        }
    }

    /*
    @Test
    public void remoteProcedureQueryDifferentLayout() throws Exception {
        // Tests that Storables can be serialized from procedure to caller,
        // where Storable layout version differs.

        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        Storage<StorableTestVersioned> storage =
            clientRepo.storageFor(StorableTestVersioned.class);

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        QueryDiffLayoutProc proc = new QueryDiffLayoutProc();
        List<Storable> reply = cap.beginCall(proc).fetchReply().toList();

        assertEquals(5, reply.size());
        for (int i=0; i<5; i++) {
            Storable s = reply.get(i);
            assertEquals(i + 5, s.getPropertyValue("id"));
            // FIXME: more
        }
    }
    */

    @Test
    public void remoteProcedureFillDifferentLayout() throws Exception {
        // Tests that Storables can be sent from caller to procedure, where
        // Storable layout version differs.

        Repository clientRepo;
        {
            Repository repo = MapRepositoryBuilder.newRepository();
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        Class<? extends Storable> newType = generateNewType();
        Storage<? extends Storable> newStorage = clientRepo.storageFor(newType);

        RemoteProcedureCapability cap = clientRepo.getCapability(RemoteProcedureCapability.class);

        FillProc proc = new FillProc();
        RemoteProcedure.Call call = cap.beginCall(proc);
        for (int i=0; i<20; i++) {
            Storable stb = newStorage.prepare();
            stb.setPropertyValue("id", i);
            stb.setPropertyValue("stringProp", "hello " + i);
            stb.setPropertyValue("intProp", 123);
            stb.setPropertyValue("longProp", 2L);
            call.send(stb);
        }
        call.execute();

        Storage<StorableTestVersioned> originalStorage =
            clientRepo.storageFor(StorableTestVersioned.class);

        Cursor<StorableTestVersioned> c = originalStorage.query().orderBy("id").fetch();
        int i = 0;
        while (c.hasNext()) {
            StorableTestVersioned stb = c.next();
            assertEquals(i, stb.getId());
            assertEquals("hello " + i, stb.getStringProp());
            assertEquals(123, stb.getIntProp());
            assertTrue(0.0 == stb.getDoubleProp());
            i++;
        }
    }

    @Test
    public void remoteResyncCapability() throws Exception {
        // Tests that ResyncCapability can be sent from remote repository
        // to client repository.

        Repository clientRepo, replicatedRepo;
        {
            ReplicatedRepositoryBuilder rrBuilder = new ReplicatedRepositoryBuilder();
            rrBuilder.setMasterRepositoryBuilder(new MapRepositoryBuilder());
            rrBuilder.setReplicaRepositoryBuilder(new MapRepositoryBuilder());
            replicatedRepo = rrBuilder.build();

            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(replicatedRepo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        ResyncCapability resyncCap = replicatedRepo.getCapability(ResyncCapability.class);
        assertNotNull(resyncCap);
        Storage<StorableTestVersioned> mainRepoStorage =
            resyncCap.getMasterRepository().storageFor(StorableTestVersioned.class);

        StorableTestVersioned stb = mainRepoStorage.prepare();
        stb.setId(1);
        stb.setStringProp("SomeValue");
        stb.setIntProp(1);
        stb.setDoubleProp(1.0);
        stb.setLongProp(1L);
        stb.setVersion(1);
        assertFalse(stb.tryLoad());
        stb.insert();

        stb = mainRepoStorage.prepare();
        stb.setId(1);
        assertTrue(stb.tryLoad());

        Storage<StorableTestVersioned> replicatedRepoStorage =
            replicatedRepo.storageFor(StorableTestVersioned.class);

        StorableTestVersioned stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(1);
        assertFalse(stbReplicated.tryLoad());

        ResyncCapability rc = (ResyncCapability)clientRepo.getCapability(ResyncCapability.class); 
        assertNotNull(rc);
        rc.resync(StorableTestVersioned.class, 1.0, "id=?", new Object[] { Integer.valueOf(1) });

        stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(1);
        assertTrue(stbReplicated.tryLoad());
    }

    @Test
    public void remoteResyncCapabilityResyncAll() throws Exception {
        // Tests that ResyncCapability can be sent from remote repository
        // to client repository.

        Repository clientRepo, replicatedRepo;
        {
            ReplicatedRepositoryBuilder rrBuilder = new ReplicatedRepositoryBuilder();
            rrBuilder.setMasterRepositoryBuilder(new MapRepositoryBuilder());
            rrBuilder.setReplicaRepositoryBuilder(new MapRepositoryBuilder());
            replicatedRepo = rrBuilder.build();

            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(replicatedRepo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        ResyncCapability resyncCap = replicatedRepo.getCapability(ResyncCapability.class);
        assertNotNull(resyncCap);
        Storage<StorableTestVersioned> mainRepoStorage =
            resyncCap.getMasterRepository().storageFor(StorableTestVersioned.class);

        StorableTestVersioned stb = mainRepoStorage.prepare();
        stb.setId(1);
        stb.setStringProp("SomeValue");
        stb.setIntProp(1);
        stb.setDoubleProp(1.0);
        stb.setLongProp(1L);
        stb.setVersion(1);
        assertFalse(stb.tryLoad());
        stb.insert();

        stb = mainRepoStorage.prepare();
        stb.setId(2);
        stb.setStringProp("SomeValue");
        stb.setIntProp(2);
        stb.setDoubleProp(2.0);
        stb.setLongProp(2L);
        stb.setVersion(2);
        assertFalse(stb.tryLoad());
        stb.insert();

        stb = mainRepoStorage.prepare();
        stb.setId(1);
        assertTrue(stb.tryLoad());

        stb = mainRepoStorage.prepare();
        stb.setId(2);
        assertTrue(stb.tryLoad());

        Storage<StorableTestVersioned> replicatedRepoStorage =
            replicatedRepo.storageFor(StorableTestVersioned.class);

        StorableTestVersioned stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(1);
        assertFalse(stbReplicated.tryLoad());

        stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(2);
        assertFalse(stbReplicated.tryLoad());

        ResyncCapability rc = (ResyncCapability)clientRepo.getCapability(ResyncCapability.class); 
        assertNotNull(rc);
        rc.resync(StorableTestVersioned.class, 1.0, null, (Object[]) null);

        stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(1);
        assertTrue(stbReplicated.tryLoad());

        stbReplicated = replicatedRepoStorage.prepare();
        stbReplicated.setId(2);
        assertTrue(stbReplicated.tryLoad());
    }

    @Test
    public void noRemoteResyncCapability() throws Exception {
        // Tests that ClientRepository returns null if the server does not have
        // any ResyncCapability.

        Repository clientRepo, mainRepo;
        {
            MapRepositoryBuilder repoBuilder = new MapRepositoryBuilder();
            mainRepo = repoBuilder.build();

            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(mainRepo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            clientRepo = ClientRepository.from(remoteRepo);
        }

        ResyncCapability rc = (ResyncCapability)clientRepo.getCapability(ResyncCapability.class);
        assertNull(rc);
    }

    private static Class<? extends Storable> generateNewType() throws SupportException {
        final String newName = StorableTestVersioned.class.getName();

        SyntheticStorableBuilder bob = new SyntheticStorableBuilder
            (newName, new ClassLoader() {
                @Override
                protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException
                {
                    if (name.equals(newName)) {
                        throw new ClassNotFoundException();
                    }
                    return super.loadClass(name, resolve);
                }
            });
        bob.setClassNameProvider(new SyntheticStorableBuilder.ClassNameProvider() {
            public String getName() {
                return newName;
            }
            public boolean isExplicit() {
                return true;
            }
        });
        bob.addProperty("id", int.class);
        bob.addPrimaryKey().addProperty("id");
        bob.addProperty("stringProp", String.class);
        bob.addProperty("version", int.class).setIsVersion(true);
        // These are still required by server.
        bob.addProperty("intProp", int.class);
        bob.addProperty("longProp", long.class);
        return bob.build();
    }

    private void fill(Storage<StorableTestVersioned> storage) throws Exception {
        for (int j = 0; j < 20; ++j) {
            StorableTestVersioned stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }
    }

    private static class EchoProc implements RemoteProcedure<String, String> {
        private static int mRequestCount;
        private static int mSuccessCount;

        public synchronized boolean handleRequest(Repository repo, Request<String, String> request)
            throws RepositoryException
        {
            synchronized (EchoProc.class) {
                mRequestCount++;
                String message = request.receive();
                if (message == null) {
                    request.finish();
                } else {
                    request.beginReply().send(message + "!").finish();
                }
                mSuccessCount++;
            }
            return true;
        }

        static synchronized void resetEchoCounts() {
            mRequestCount = 0;
            mSuccessCount = 0;
        }

        static synchronized void assertEchoCounts(int requestCount, int successCount) {
            assertEquals("request count", requestCount, mRequestCount);
            assertEquals("success count", successCount, mSuccessCount);
        }
    }

    private static class InsertProc implements RemoteProcedure<Object, Object> {
        private final int mId;

        InsertProc(int id) {
            mId = id;
        }

        public synchronized boolean handleRequest(Repository repo, Request<Object, Object> request)
            throws RepositoryException
        {
            Storage<StorableTestVersioned> storage = repo.storageFor(StorableTestVersioned.class);
            StorableTestVersioned stb = storage.prepare();
            stb.setId(mId);
            stb.setStringProp("hello");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
            return true;
        }
    }

    private static class QueryProc implements RemoteProcedure<StorableTestVersioned, Object> {
        private final String mFilter;
        private final Object[] mParams;

        QueryProc(String filter, Object... params) {
            mFilter = filter;
            mParams = params;
        }

        public boolean handleRequest(Repository repo,
                                     Request<StorableTestVersioned, Object> request)
            throws RepositoryException
        {
            Storage<StorableTestVersioned> storage = repo.storageFor(StorableTestVersioned.class);
            Cursor<StorableTestVersioned> c = storage.query(mFilter).withValues(mParams).fetch();
            request.beginReply().sendAll(c).finish();
            return true;
        }
    }

    /*
    private static class QueryDiffLayoutProc implements RemoteProcedure<Storable, Object> {
        QueryDiffLayoutProc() {
        }

        public boolean handleRequest(Repository repo, Request<Storable, Object> request)
            throws RepositoryException
        {
            Class<? extends Storable> newType = generateNewType();
            Storage<? extends Storable> newStorage = repo.storageFor(newType);

            for (int i=0; i<5; i++) {
                Storable stb = newStorage.prepare();
                stb.setPropertyValue("id", i + 5);
                stb.setPropertyValue("stringProp", "hello " + (i + 5));
                stb.setPropertyValue("intProp", 123);
                stb.setPropertyValue("longProp", 2L);
                stb.insert();
            }

            Cursor<? extends Storable> c = newStorage.query().fetch();
            request.beginReply().sendAll(c).finish();
            return true;
        }
    }
    */

    private static class FillProc implements RemoteProcedure<Object, StorableTestVersioned> {
        FillProc() {
        }

        public boolean handleRequest(Repository repo,
                                     Request<Object, StorableTestVersioned> request)
            throws RepositoryException
        {
            StorableTestVersioned stb;
            while ((stb = request.receive()) != null) {
                stb.insert();
            }
            return true;
        }
    }
}
