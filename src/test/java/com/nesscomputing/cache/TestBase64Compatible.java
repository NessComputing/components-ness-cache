/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.cache;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class TestBase64Compatible
{
    // Make sure that Apache Base64 can read what we encode.
    @Test
    public void testEncoding()
    {
        final Random rand = new Random();

        for (int i = 0; i < 100; i++) {
            final byte [] bytes = new byte[i];
            rand.nextBytes(bytes);

            final byte [] base64Bytes = Base64.encode(bytes);
            final byte [] decoded = new org.apache.commons.codec.binary.Base64().decode(base64Bytes);

            Assert.assertEquals(bytes.length, decoded.length);
            for (int j = 0; j < bytes.length; j++) {
                Assert.assertEquals(bytes[j], decoded[j]);
            }

        }
    }

    // Make sure that we can read what Apache Base64 encodes.
    @Test
    public void testDecode()
    {
        final Random rand = new Random();

        for (int i = 0; i < 100; i++) {
            final byte [] bytes = new byte[i];
            rand.nextBytes(bytes);

            final byte [] base64Bytes = new org.apache.commons.codec.binary.Base64().encode(bytes);
            final byte [] decoded = Base64.decode(base64Bytes);

            Assert.assertEquals(bytes.length, decoded.length);
            for (int j = 0; j < bytes.length; j++) {
                Assert.assertEquals(bytes[j], decoded[j]);
            }

        }
    }

    // Make sure that values encoded are actually identical.
    @Test
    public void testEqualEncoding()
    {
        final Random rand = new Random();

        for (int i = 0; i < 100; i++) {
            final byte [] bytes = new byte[i];
            rand.nextBytes(bytes);

            final byte [] base64Bytes = Base64.encode(bytes);
            final byte [] apache64Bytes = new org.apache.commons.codec.binary.Base64().encode(bytes);

            Assert.assertEquals(base64Bytes.length, apache64Bytes.length);
            for (int j = 0; j < base64Bytes.length; j++) {
                Assert.assertEquals(base64Bytes[j], apache64Bytes[j]);
            }
        }
    }
}