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

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.SupportException;

import com.amazon.carbonado.gen.DetachedStorableFactory;
import com.amazon.carbonado.gen.StorableCopier;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class StorableWriter<S extends Storable> {
    static final StorableWriter<Storable> DEFAULT = new Default();

    StorableWriter() {
    }

    abstract void writeForLoad(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeForInsert(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeForUpdate(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeForDelete(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeLoadResponse(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeInsertResponse(S storable, OutputStream out)
        throws IOException, SupportException;

    abstract void writeUpdateResponse(S storable, OutputStream out)
        throws IOException, SupportException;

    static class Copier<S extends Storable> extends StorableWriter<S> {
        private final StorableCopier<S, Storable> mCopier;
        private final DetachedStorableFactory<?> mFactory;

        Copier(StorableCopier<S, Storable> copier, DetachedStorableFactory<?> factory) {
            mCopier = copier;
            mFactory = factory;
        }

        @Override
        void writeForLoad(S storable, OutputStream out) throws IOException, SupportException {
            // TODO: just write the primary or alternate keys somehow
            Storable target = mFactory.newInstance();
            mCopier.copyAllProperties(storable, target);
            target.writeTo(out);
        }

        @Override
        void writeForInsert(S storable, OutputStream out) throws IOException, SupportException {
            Storable target = mFactory.newInstance();
            mCopier.copyAllProperties(storable, target);
            target.writeTo(out);
        }

        @Override
        void writeForUpdate(S storable, OutputStream out) throws IOException, SupportException {
            Storable target = mFactory.newInstance();
            mCopier.copyPrimaryKeyProperties(storable, target);
            mCopier.copyDirtyProperties(storable, target);
            target.writeTo(out);
        }

        @Override
        void writeForDelete(S storable, OutputStream out) throws IOException, SupportException {
            Storable target = mFactory.newInstance();
            mCopier.copyPrimaryKeyProperties(storable, target);
            target.writeTo(out);
        }

        @Override
        void writeLoadResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            Storable target = mFactory.newInstance();
            mCopier.copyAllProperties(storable, target);
            target.markAllPropertiesClean();
            target.writeTo(out);
        }

        @Override
        void writeInsertResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            // TODO: only write back what changed somehow
            Storable target = mFactory.newInstance();
            mCopier.copyAllProperties(storable, target);
            target.markAllPropertiesClean();
            target.writeTo(out);
        }

        @Override
        void writeUpdateResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            // TODO: only write back what changed somehow
            Storable target = mFactory.newInstance();
            mCopier.copyAllProperties(storable, target);
            target.markAllPropertiesClean();
            target.writeTo(out);
        }
    }

    private static class Default<S extends Storable> extends StorableWriter<S> {
        Default() {
        }

        @Override
        void writeForLoad(S storable, OutputStream out) throws IOException, SupportException {
            // TODO: just write the primary or alternate keys somehow
            storable.writeTo(out);
        }

        @Override
        void writeForInsert(S storable, OutputStream out) throws IOException, SupportException {
            storable.writeTo(out);
        }

        @Override
        void writeForUpdate(S storable, OutputStream out) throws IOException, SupportException {
            // TODO: just write the primary keys and dirty properties somehow
            storable.writeTo(out);
        }

        @Override
        void writeForDelete(S storable, OutputStream out) throws IOException, SupportException {
            // TODO: just write the primary keys somehow
            storable.writeTo(out);
        }

        @Override
        void writeLoadResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            storable.writeTo(out);
        }

        @Override
        void writeInsertResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            // TODO: only write back what changed somehow
            storable.writeTo(out);
        }

        @Override
        void writeUpdateResponse(S storable, OutputStream out)
            throws IOException, SupportException
        {
            // TODO: only write back what changed somehow
            storable.writeTo(out);
        }
    }
}
