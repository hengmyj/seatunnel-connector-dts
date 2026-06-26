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

    @Test
    public void testEscapeJson() {
        Assert.assertEquals("a\\nb", DtsValueMapper.escapeJson("a\nb"));
        Assert.assertEquals("say \\\"hi\\\"", DtsValueMapper.escapeJson("say \"hi\""));
    }

    @Test
    public void testTruncateText() {
        Assert.assertEquals("ab...[truncated]", DtsValueMapper.truncateText("abcdef", 2));
        Assert.assertEquals("abcdef", DtsValueMapper.truncateText("abcdef", 0));
    }
}
