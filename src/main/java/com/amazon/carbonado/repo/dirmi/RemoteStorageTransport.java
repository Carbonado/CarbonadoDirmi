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

import com.amazon.carbonado.Storable;

import com.amazon.carbonado.layout.Layout;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteStorageTransport extends StorableTypeTransport {
    private final RemoteStorage mStorage;

    RemoteStorageTransport(Class<? extends Storable> type, Layout layout, RemoteStorage storage) {
        super(type, layout);
        mStorage = storage;
    }

    RemoteStorage getRemoteStorage() {
        return mStorage;
    }
}
