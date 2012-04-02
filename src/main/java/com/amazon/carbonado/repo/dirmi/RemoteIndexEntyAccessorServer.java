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
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.info.Direction;
import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.repo.indexed.IndexEntryAccessor;

/**
 * 
 * @author Jesse Morgan
 */
class RemoteIndexEntyAccessorServer<S extends Storable> implements RemoteIndexEntryAccessor<S> {

    private final IndexEntryAccessor<S> mAccessor; 
    
    public RemoteIndexEntyAccessorServer(IndexEntryAccessor<S> indexEntryAccessor) {
       mAccessor = indexEntryAccessor;
    }
    
    @Override
    public void repair(double desiredSpeed) throws RepositoryException {
        mAccessor.repair(desiredSpeed);
    }
}
