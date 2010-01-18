/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.dirmi;

import org.cojen.dirmi.Environment;

import com.amazon.carbonado.*;

import com.amazon.carbonado.stored.StorableTestBasic;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestClient {
    public static void main(String[] args) throws Exception {
        RemoteRepository remote = (RemoteRepository)
            new Environment().newSessionConnector(args[0], 3456).connect().receive();
        Repository repo = ClientRepository.from(remote);

        Storage<StorableTestBasic> storage = repo.storageFor(StorableTestBasic.class);
        StorableTestBasic stb = storage.prepare();
        stb.setId(2);
        if (!stb.tryLoad()) {
            stb.setStringProp("world");
            stb.setIntProp(321);
            stb.setLongProp(313244232323432L);
            stb.setDoubleProp(1.423423);
            stb.tryInsert();
        }
        System.out.println(stb);

        Query<StorableTestBasic> query = storage.query();
        System.out.println(query.fetch().toList());

        if (args.length > 1) {
            query = storage.query(args[1]).with(Integer.parseInt(args[2]));
            query.printPlan();
            System.out.println(query.fetch().toList());
        }

        Transaction txn = repo.enterTransaction();
        try {
            stb.setDate(new org.joda.time.DateTime());
            stb.update();
            stb.setDate(new org.joda.time.DateTime());
            stb.update();

            System.out.println("Sleeping");
            Thread.sleep(5000);

            txn.commit();
        } finally {
            txn.exit();
        }

        System.out.println(stb);
    }
}
