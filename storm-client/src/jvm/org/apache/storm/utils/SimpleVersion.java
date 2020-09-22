/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Take a version string and parse out a Major.Minor version
 */
public class SimpleVersion implements Comparable <SimpleVersion> {
    private final int _major;
    private final int _minor;
    
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)[.-_]+(\\d+).*");
    
    public SimpleVersion(String version) {
        Matcher m = VERSION_PATTERN.matcher(version);
        int maj = -1;
        int min = -1;
        if (!m.matches()) {
            //Unknown should only happen during compilation or some unit tests.
            if (!"Unknown".equals(version)) {
                throw new IllegalArgumentException("Cannot parse " + version);
            }
        } else {
            maj = Integer.valueOf(m.group(1));
            min = Integer.valueOf(m.group(2));
        }
        _major = maj;
        _minor = min;
    }
    
    public int getMajor() {
        return _major;
    }
    
    public int getMinor() {
        return _minor;
    }
    
    @Override
    public int hashCode() {
        return (Integer.hashCode(_major) * 17) & Integer.hashCode(_minor);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        
        if (!(o instanceof SimpleVersion)) {
            return false;
        }
        
        return compareTo((SimpleVersion)o) == 0;
    }
    
    @Override
    public int compareTo(SimpleVersion o) {
        int ret = Integer.compare(_major, o._major);
        if (ret == 0) {
            ret = Integer.compare(_minor, o._minor);
        }
        return ret;
    }
    
    @Override
    public String toString() {
        return _major + "." + _minor;
    }
}
