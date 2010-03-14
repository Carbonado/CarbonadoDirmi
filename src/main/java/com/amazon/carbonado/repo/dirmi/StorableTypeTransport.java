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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;

import com.amazon.carbonado.repo.map.MapRepositoryBuilder;

import com.amazon.carbonado.layout.Layout;
import com.amazon.carbonado.layout.LayoutFactory;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class StorableTypeTransport implements Serializable {
    private final Class<? extends Storable> mType;
    private volatile transient Layout mLayout;

    StorableTypeTransport(Class<? extends Storable> type, Layout layout) {
        mType = type;
        mLayout = layout;
    }

    Class<? extends Storable> getStorableType() {
        return mType;
    }

    Layout getLayout() {
        return mLayout;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        try {
            mLayout.writeTo(out);
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
            mLayout = new LayoutFactory(MapRepositoryBuilder.newRepository()).readLayoutFrom(in);
        } catch (RepositoryException e) {
            throw new IOException(e);
        }
    }
}
