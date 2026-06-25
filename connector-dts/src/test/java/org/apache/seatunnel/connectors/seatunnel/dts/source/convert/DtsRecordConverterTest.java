package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DtsRecordConverterTest {

    @Test
    public void testTableWhitelistEmptyAllowsAll() {
        DtsRecordConverter converter = new DtsRecordConverter(Collections.emptySet());
        Assert.assertTrue(converter.isTableAllowed("db1", "t1"));
    }

    @Test
    public void testTableWhitelistMatch() {
        Set<String> whitelist = new HashSet<>();
        whitelist.add("db1.t1");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist);
        Assert.assertTrue(converter.isTableAllowed("db1", "t1"));
        Assert.assertTrue(converter.isTableAllowed("DB1", "T1"));
        Assert.assertFalse(converter.isTableAllowed("db1", "t2"));
        Assert.assertFalse(converter.isTableAllowed("db2", "t1"));
    }
}
