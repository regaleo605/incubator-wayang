/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Iterates all combinations, i.e., the Cartesian product, of given {@link Iterable}s.
 */
public class CrossProductIterable<T> implements Iterable<List<T>> {

    public final List<? extends Iterable<T>> iterables;

    public CrossProductIterable(List<? extends Iterable<T>> iterables) {
        this.iterables = new ArrayList<>(iterables);
    }

    @Override
    public java.util.Iterator<List<T>> iterator() {
        return new CrossProductIterable.Iterator<>(this);
    }

    private static class Iterator<T> implements java.util.Iterator<List<T>> {

        private final CrossProductIterable<T> crossProductIterable;

        private final List<java.util.Iterator<T>> partialIterators;

        private List<T> vals;

        private boolean hasEmptyIterator;


        private Iterator(CrossProductIterable<T> crossProductIterable) {
            // Initialize.
            this.crossProductIterable = crossProductIterable;
            this.partialIterators = new ArrayList<>(this.crossProductIterable.iterables.size());
            this.vals = new ArrayList<>(this.crossProductIterable.iterables.size());

            for (Iterable<T> iterable : this.crossProductIterable.iterables) {
                final java.util.Iterator<T> iterator = iterable.iterator();
                this.partialIterators.add(iterator);
                this.hasEmptyIterator |= !iterator.hasNext();
                this.vals.add(null);
            }

        }

        @Override
        public boolean hasNext() {
            if (this.hasEmptyIterator) return false;
            for (java.util.Iterator<T> partialIterator : this.partialIterators) {
                if (partialIterator.hasNext()) return true;
            }
            return false;
        }

        @Override
        public List<T> next() {
            assert this.hasNext();

            List<T> next = new ArrayList<>(this.partialIterators.size());
            boolean isFetchNext = true;
            for (int i = 0; i < this.partialIterators.size(); i++) {
                java.util.Iterator<T> partialIterator = this.partialIterators.get(i);
                if (isFetchNext) {
                    // If the Iterator has made a full pass, replace it with a new one.
                    boolean isFullPass;
                    if (isFullPass = !partialIterator.hasNext()) {
                        assert i < this.partialIterators.size() - 1;
                        partialIterator = this.crossProductIterable.iterables.get(i).iterator();
                        this.partialIterators.set(i, partialIterator);
                        assert partialIterator.hasNext();
                    }

                    // If the Iterator had made a full pass or this is the very first iteration.
                    if (isFetchNext) {
                        isFetchNext = isFullPass || this.vals.get(i) == null;
                        this.vals.set(i, partialIterator.next());
                    }
                }

                next.add(this.vals.get(i));
            }

            return next;
        }
    }
}
