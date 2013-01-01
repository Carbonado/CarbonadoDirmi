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

package com.amazon.carbonado.repo.dirmi;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.reflect.Method;

import java.util.Map;
import java.util.UUID;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.repo.map.MapRepositoryBuilder;
import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutFactory;

import com.amazon.carbonado.gen.DetachedStorableFactory;
import com.amazon.carbonado.gen.StorableCopier;

import com.amazon.carbonado.util.SoftValuedCache;

/**
 * Cache of reconstructed classes corresponding to a Layout.
 *
 * @author Brian S O'Neill
 */
class ReconstructedCache {
    static final ReconstructedCache THE = new ReconstructedCache();

    // Keep a reference to LayoutFactory instance, since it generates classes,
    // sometimes indirectly. Keeping the same instance prevents classes from
    // being redefined. These classes might also be non-unloadable, leading to
    // class loading memory leaks.
    final LayoutFactory mLayoutFactory;

    private final SoftValuedCache<StorableLayoutKey, Class> mCache;

    private ReconstructedCache() {
        try {
            // Favor in-memory Tupl repository, but fallback to BDB-JE, and
            // then to the Map repository. Map repository has locking defects,
            // which is why it is the least preferred. BDB-JE isn't suitable
            // for long-term in-memory storage, because it never cleans out old
            // log entries. Tupl is the better choice overall because it
            // implements locks correctly and doesn't leak memory.

            Repository repo;
            File tmpDir = null;

            try {
                repo = (Repository) Class.forName
                    ("com.amazon.carbonado.repo.tupl.TuplRepositoryBuilder")
                    .getMethod("newRepository").invoke(null);
            } catch (Throwable e) {
                tmpDir = new File(System.getProperty("java.io.tmpdir"),
                                  "CarbonadoDirmi-" + UUID.randomUUID());

                try {
                    BDBRepositoryBuilder b = new BDBRepositoryBuilder();
                    b.setName("ReconstructedCache");
                    b.setEnvironmentHomeFile(tmpDir);
                    b.setLogInMemory(true);
                    b.setCacheSize(1000000);
                    b.setProduct("JE");
                    b.setTransactionNoSync(true);

                    repo = b.build();
                } catch (Throwable e2) {
                    repo = MapRepositoryBuilder.newRepository();
                }
            }

            if (tmpDir != null && tmpDir.exists()) {
                final File fTmpDir = tmpDir;
                final Repository fRepo = repo;

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            fRepo.close();
                        } finally {
                            deleteTempDir(fTmpDir);
                        }
                    }
                });
            }

            mLayoutFactory = new LayoutFactory(repo);
        } catch (RepositoryException e) {
            // MapRepository shouldn't throw a RepositoryException.
            throw new AssertionError(e);
        }

        mCache = SoftValuedCache.newCache(3);
    }

    static void deleteTempDir(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                deleteTempDir(f);
            }
        }
        // Need to retry a few times, because repository might be concurrently shutting down.
        for (int i=0; i<50; i++) {
            if (!file.exists() || file.delete()) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    Layout layoutFor(Class<? extends Storable> type) throws RepositoryException {
        synchronized (mLayoutFactory) {
            return mLayoutFactory.layoutFor(type);
        }
    }

    /**
     * Reconstructs the given layout, or returns null if the given type
     * already matches.
     */
    Class reconstruct(Class<? extends Storable> type, Layout layout) throws RepositoryException {
        if (layoutFor(type).equalLayouts(layout)) {
            return null;
        }

        // Protocol version doesn't matter.
        StorableLayoutKey key = new StorableLayoutKey(0, type, layout);

        synchronized (this) {
            Class clazz = mCache.get(key);
            if (clazz == null) {
                clazz = layout.reconstruct(type.getClassLoader());
                mCache.put(key, clazz);
            }
            return clazz;
        }
    }

    <S extends Storable> StorableWriter<S> writerFor(Class<S> type, Layout layout)
        throws RepositoryException
    {
        Class target = reconstruct(type, layout);
        if (target == null) {
            return (StorableWriter<S>) StorableWriter.DEFAULT;
        }
        StorableCopier<S, Storable> copier = StorableCopier.from(type).to(target);
        DetachedStorableFactory<?> factory = new DetachedStorableFactory(target);
        return new StorableWriter.Copier<S>(copier, factory);
    }
}
