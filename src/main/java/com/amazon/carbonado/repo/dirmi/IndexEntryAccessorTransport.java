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

import java.io.Serializable;

import com.amazon.carbonado.Storable;
import com.amazon.carbonado.capability.IndexInfo;
import com.amazon.carbonado.info.Direction;

/**
 * Class to carry a RemoteIndexEntryAccessor and associated IndexInfo back to a client.
 * 
 * @author Jesse Morgan
 */
public class IndexEntryAccessorTransport<S extends Storable> implements Serializable, IndexInfo {

    /**
     * Serial UID.
     */
    private static final long serialVersionUID = -4220785607475227254L;
    
    
    private final RemoteIndexEntryAccessor<S> mRemote;
    
    private final String mName;
    private final boolean mUnique;
    private final boolean mClustered;
    private final String[] mPropertyNames;
    private final Direction[] mProperyDirections;
    
    IndexEntryAccessorTransport(RemoteIndexEntryAccessor<S> remote, IndexInfo indexInfo) {
        mRemote = remote;
        mName = indexInfo.getName();
        mUnique = indexInfo.isUnique();
        mClustered = indexInfo.isClustered();
        mPropertyNames = indexInfo.getPropertyNames();
        mProperyDirections = indexInfo.getPropertyDirections();
    }
    
    public RemoteIndexEntryAccessor<S> getRemote() {
        return mRemote;
    }
    
    @Override
    public String getName() {
        return mName;
    }

    @Override
    public boolean isUnique() {
        return mUnique;
    }

    @Override
    public boolean isClustered() {
        return mClustered;
    }

    @Override
    public String[] getPropertyNames() {
        return mPropertyNames;
    }

    @Override
    public Direction[] getPropertyDirections() {
        return mProperyDirections;
    }
}
