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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.logging.LogFactory;

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
    private static final long serialVersionUID = -2052346822135818736L;

    private static final int LAYOUT_FACTORY_VERSION;

    static {
        int version = 1;
        try {
            version = LayoutFactory.VERSION;
        } catch (NoSuchFieldError e) {
        }
        LAYOUT_FACTORY_VERSION = version;
    }

    private final int mProtocolVersion;
    private final Class<? extends Storable> mType;
    private volatile transient Layout mLayout;

    StorableTypeTransport(Class<? extends Storable> type, Layout layout) {
        // 0:  Original protocol version.
        // 1:  Use RemoteStorageServer.CURSOR_START marker. Obsolete.
        // -1: Doesn't write start marker and fetch doesn't block waiting for first result.
        //     Note: New protocol versions must go negative, as a workaround for
        //           older code which had a >= version check.
        this(-1, type, layout);
    }

    StorableTypeTransport(int protocolVersion, Class<? extends Storable> type, Layout layout) {
        mProtocolVersion = protocolVersion;
        mType = type;
        mLayout = layout;
    }

    Class<? extends Storable> getStorableType() {
        return mType;
    }

    int getProtocolVersion() {
        return mProtocolVersion;
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
            LayoutFactory factory;
            if (LAYOUT_FACTORY_VERSION == 1) {
                // Older version of Carbonado cannot tolerate any property
                // changes, even if irrelevant. A new repository must be used
                // each time, which leads to excessive generated classes.
                // Updates to Carbonado and CarbonadoDirmi are required.
                factory = new LayoutFactory(MapRepositoryBuilder.newRepository());
            } else {
                factory = ReconstructedCache.THE.mLayoutFactory;
            }
            synchronized (factory) {
                mLayout = factory.readLayoutFrom(in);
            }
        } catch (RepositoryException e) {
            // Something needs to be logged, because an IOException destroys
            // the stream, making it more difficult to report the cause.
            LogFactory.getLog(StorableTypeTransport.class).error
                ("Unable to transport Storable layout information for " + mType.getName(), e);
            throw new IOException(e);
        }
    }
}
