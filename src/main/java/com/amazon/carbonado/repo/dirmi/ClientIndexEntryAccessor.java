/*
 * Copyright 2012 Amazon Technologies, Inc. or its affiliates.
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

import java.util.Comparator;

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.SupportException;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.repo.indexed.IndexEntryAccessor;

/**
 * 
 * @author Jesse Morgan
 */
class ClientIndexEntryAccessor<S extends Storable> implements IndexEntryAccessor<S> {

    private final RemoteIndexEntryAccessor<S> mRemote;
    private final IndexInfo mIndexInfo;
    
    public ClientIndexEntryAccessor(IndexEntryAccessorTransport<S> indexEntryAccessorTransport) throws SupportException {
        mIndexInfo = indexEntryAccessorTransport;
        mRemote = indexEntryAccessorTransport.getRemote();
    }

    @Override
    public String getName() {
        return mIndexInfo.getName();
    }

    @Override
    public boolean isUnique() {
        return mIndexInfo.isUnique();
    }

    @Override
    public boolean isClustered() {
        return mIndexInfo.isClustered();
    }

    @Override
    public String[] getPropertyNames() {
        return mIndexInfo.getPropertyNames();
    }

    @Override
    public Direction[] getPropertyDirections() {
        return mIndexInfo.getPropertyDirections();
    }

    @Override
    public Storage<?> getIndexEntryStorage() {
        throw new UnsupportedOperationException("getIndexEntryStorage is not implemented for ClientRepository");
    }

    @Override
    public void copyToMasterPrimaryKey(Storable indexEntry, S master)
            throws FetchException {
        
        throw new UnsupportedOperationException("copyToMasterPrimaryKey is not implemented for ClientRepository");
    }

    @Override
    public void copyFromMaster(Storable indexEntry, S master)
            throws FetchException {
        throw new UnsupportedOperationException("copyFromMaster is not implemented for ClientRepository");
    }

    @Override
    public boolean isConsistent(Storable indexEntry, S master)
            throws FetchException {
        throw new UnsupportedOperationException("isConsistent is not implemented for ClientRepository");
    }

    @Override
    public void repair(double desiredSpeed) throws RepositoryException {
        mRemote.repair(desiredSpeed);
    }

    @Override
    public Comparator<? extends Storable> getComparator() {
        throw new UnsupportedOperationException("getComparator is not implemented for ClientRepository");
    }

}
