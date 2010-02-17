/*
 * Copyright 2010 Amazon Technologies, Inc. or its affiliates.
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

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

import org.junit.*;

import com.amazon.carbonado.repo.dirmi.*;

import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Session;

import com.amazon.carbonado.sequence.SequenceCapability;
import com.amazon.carbonado.sequence.SequenceValueProducer;
import com.amazon.carbonado.stored.StorableTestVersioned;
import com.amazon.carbonado.*;

/**
 * 
 *
 * @author Olga Kuznetsova
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
    public void mapRepoInsertTest() {
        try{
            Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
            
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
            
            Storage <StorableTestVersioned> clientstorage = clientRepo.storageFor(StorableTestVersioned.class);
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
        } catch (Exception e) {
            assertTrue(false);
        }
    }

    @Test
    public void anotherTest() throws Exception {
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
            
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
    }

    @Test
    public void transactionsTest() throws Exception {
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        final Repository clientRepo = ClientRepository.from(remoteRepo);

        final Storage<StorableTestVersioned> clientStorage = clientRepo.storageFor(StorableTestVersioned.class);
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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        final Repository clientRepo = ClientRepository.from(remoteRepo);

        final Storage<StorableTestVersioned> clientStorage = clientRepo.storageFor(StorableTestVersioned.class);
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
                        //      txn.attach();
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
    public void querriesTest() throws Exception { 
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage = clientRepo.storageFor(StorableTestVersioned.class);
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
    public void sequenceGeneratorTest() { 

    }

    @Test
    public void exceptionTest() {

    }

    @Test
    public void threadTest() {

    }

    @Test
    public void closingTest() throws Exception {
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);

        Storage<StorableTestVersioned> clientStorage = clientRepo.storageFor(StorableTestVersioned.class);
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

        Storage<StorableTestVersioned> actualStorage = repo.storageFor(StorableTestVersioned.class);
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
            Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
            repos.add(repo);
            Session[] pair = new Environment().newSessionPair();
            pair[0].send(RemoteRepositoryServer.from(repo));
            RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
            Repository clientRepo = ClientRepository.from(remoteRepo);
            Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
            StorableTestVersioned stb;
            for (int j = 0; j < 20; ++j) {
                stb = storage.prepare();
                stb.setId(j);
                stb.setStringProp(j + "world");
                stb.setIntProp(321);
                stb.setLongProp(313244232323432L);
                stb.setDoubleProp(1.423423);
                stb.insert();
            }

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

            Storage<StorableTestVersioned> storage1 = clientRepo.storageFor(StorableTestVersioned.class);;
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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        for (int j = 0; j < 20; ++j) {
            stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        for (int j = 0; j < 20; ++j) {
            stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        for (int j = 0; j < 20; ++j) {
            stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        for (int j = 0; j < 20; ++j) {
            stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

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
        Repository repo = com.amazon.carbonado.repo.map.MapRepositoryBuilder.newRepository();
        Session[] pair = new Environment().newSessionPair();
        pair[0].send(RemoteRepositoryServer.from(repo));
        RemoteRepository remoteRepo = (RemoteRepository) pair[1].receive();
        Repository clientRepo = ClientRepository.from(remoteRepo);
        Storage<StorableTestVersioned> storage = clientRepo.storageFor(StorableTestVersioned.class);
        StorableTestVersioned stb;
        for (int j = 0; j < 20; ++j) {
            stb = storage.prepare();
            stb.setId(j);
            stb.setStringProp(j + "world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.insert();
        }

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
}
