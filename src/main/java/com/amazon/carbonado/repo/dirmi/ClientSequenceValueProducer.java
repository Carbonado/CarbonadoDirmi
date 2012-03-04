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

import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;

import com.amazon.carbonado.sequence.AbstractSequenceValueProducer;

/**
 * 
 *
 * @author Olga Kuznetsova
 */
class ClientSequenceValueProducer extends AbstractSequenceValueProducer {
    private volatile RemoteSequenceValueProducer mProducer;
    public ClientSequenceValueProducer(RemoteSequenceValueProducer remote) {
	mProducer = remote;
    }

    public void reconnect(RemoteSequenceValueProducer remote) {
	mProducer = remote;
    }

    @Override
    public int nextIntValue() throws PersistException {
	return mProducer.nextIntValue();
    }

    @Override
    public String nextDecimalValue() throws PersistException {
	return mProducer.nextDecimalValue();
    }

    @Override
    public String nextNumericalValue(int radix, int minLength) 
	throws PersistException {
	return mProducer.nextNumericalValue(radix, minLength);
    }
 
    @Override
    public long nextLongValue() throws PersistException {
	return mProducer.nextLongValue();
    }
   
    @Override
    public boolean returnReservedValues() throws FetchException, PersistException {
	return mProducer.returnReservedValues();
    }
}