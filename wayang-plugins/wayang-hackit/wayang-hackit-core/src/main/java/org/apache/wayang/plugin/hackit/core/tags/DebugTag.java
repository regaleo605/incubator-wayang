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
package org.apache.wayang.plugin.hackit.core.tags;

/**
 * DebugTag is the {@link HackitTag} that identifies the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 * as an element that needs to be captured with its timestamp, and send it out for further analysis
 */
public class DebugTag extends HackitTag  {

    /**
     * Seed is the element that allows to have small overhead in memory, it is like using singleton.
     */
    private static DebugTag seed = null;

    /**
     * Default constructor
     */
    public DebugTag(){
        super();
    }

    @Override
    public boolean isSendOut() {
        //TODO: set the correct value to this return
        return true;
    }

    @Override
    public boolean isSkip() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public boolean isHaltJob() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public boolean hasCallback() {
        //TODO: set the correct value to this return
        return true;
    }

    @Override
    public HackitTag getInstance() {
        if(seed == null){
            seed = new DebugTag();
        }
        return seed;
    }

    @Override
    public int hashCode() {
        return 2;
    }

    @Override
    public String toString(){
        return "DebugTag";
    }
}
