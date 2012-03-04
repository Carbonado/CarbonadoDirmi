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

package com.amazon.carbonado.stored;

import org.joda.time.DateTime;

import com.amazon.carbonado.Independent;
import com.amazon.carbonado.Nullable;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Version;

/**
 * @author Brian S O'Neill (boneill)
 */
@PrimaryKey("id")
public abstract class StorableTestVersioned implements Storable {
    public abstract int getId();
    public abstract void setId(int id);

    // Basic coverage of the primitives
    public abstract String getStringProp();
    public abstract void setStringProp(String aStringThing);

    public abstract int getIntProp();
    public abstract void setIntProp(int anInt);

    public abstract long getLongProp();
    public abstract void setLongProp(long aLong);

    @Independent
    public abstract double getDoubleProp();
    public abstract void setDoubleProp(double aDouble);

    @Nullable
    public abstract DateTime getDate();
    public abstract void setDate(DateTime aDate);

    @Version
    public abstract int getVersion();
    public abstract void setVersion(int version);
}
