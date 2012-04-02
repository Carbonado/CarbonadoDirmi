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

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.capability.IndexInfoCapability;
import com.amazon.carbonado.repo.indexed.IndexEntryAccessCapability;
import com.amazon.carbonado.repo.indexed.IndexEntryAccessor;

/**
 * 
 * @author Jesse Morgan
 *
 */
class RemoteIndexEntryAccessCapabilityServer implements
        RemoteIndexEntryAccessCapability {

    private final IndexEntryAccessCapability mAccessCapability;
    
    public RemoteIndexEntryAccessCapabilityServer(IndexEntryAccessCapability cap) {
        mAccessCapability = cap;
    }
    
    @Override
    public <S extends Storable> IndexEntryAccessorTransport<S>[] getIndexEntryAccessors(
            Class<S> storableType) throws RepositoryException {
        
        final IndexEntryAccessor<S>[] locals = mAccessCapability.getIndexEntryAccessors(storableType);
        final IndexEntryAccessorTransport<S>[] remotes = new IndexEntryAccessorTransport[locals.length];
        
        for (int i = 0; i < locals.length; i++) {
            remotes[i] = new IndexEntryAccessorTransport<S>(new RemoteIndexEntyAccessorServer(locals[i]), locals[i]);
        }
        
        return remotes;
    }

}
