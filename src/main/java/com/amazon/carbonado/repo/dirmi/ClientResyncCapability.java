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

import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.capability.ResyncCapability;

class ClientResyncCapability implements ResyncCapability {
    private final RemoteResyncCapability remoteResyncCap;

    public ClientResyncCapability(RemoteResyncCapability rrc) {
        remoteResyncCap = rrc;
    }
    
    @Override
    public Repository getMasterRepository() {
        throw new UnsupportedOperationException
            ("Getting master repository is not supported in ClientRepository");
    }

    @Override
    public <S extends Storable> void resync(Class<S> type, double desiredSpeed,
                                            String filter, Object... filterValues)
        throws RepositoryException
    {
        remoteResyncCap.resync( type, desiredSpeed, filter, filterValues );
    }

    @Override
    public <S extends Storable> void resync(Class<S> type, Listener<? super S> listener,
                                            double desiredSpeed,
                                            String filter, Object... filterValues)
        throws RepositoryException
    {
        throw new UnsupportedOperationException
            ("Resync callbacks are not supported by this repository yet");
    }
}
