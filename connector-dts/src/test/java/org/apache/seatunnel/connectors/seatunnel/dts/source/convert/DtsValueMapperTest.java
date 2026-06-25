package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import org.junit.Assert;
import org.junit.Test;

public class DtsValueMapperTest {

    @Test
    public void testToJsonValue() {
        Assert.assertEquals("null", DtsValueMapper.toJsonValue(null));
        Assert.assertEquals("42", DtsValueMapper.toJsonValue(42L));
        Assert.assertEquals("\"hello\"", DtsValueMapper.toJsonValue("hello"));
        Assert.assertEquals("true", DtsValueMapper.toJsonValue(true));
    }
}
