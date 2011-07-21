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

package com.amazon.carbonado.repo.dirmi;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Map;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.repo.map.MapRepositoryBuilder;

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
            mLayoutFactory = new LayoutFactory(MapRepositoryBuilder.newRepository());
        } catch (RepositoryException e) {
            // MapRepository shouldn't throw a RepositoryException.
            throw new AssertionError(e);
        }

        mCache = SoftValuedCache.newCache(3);
    }

    Layout layoutFor(Class<? extends Storable> type) throws RepositoryException {
        return mLayoutFactory.layoutFor(type);
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
