package com.instaclustr.kafka.connect.stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UtilTest {

    @Test
    public void testStreamIterate() {
        Assert.assertEquals(Stream.iterate(1, x -> x <= 3, x -> x + 1).map(x -> x * 10).collect(Collectors.toList()),
                List.of(10, 20, 30));
    }

    @Test
    public void testStreamDoWhile() {
        Assert.assertEquals(Util.doWhile(1, x -> x <= 3, x -> x + 1).map(x -> x * 10).collect(Collectors.toList()),
                List.of(10, 20, 30, 40));

        Assert.assertEquals(Util.doWhile(1, x -> x < 0, x -> x + 1).map(x -> x * 10).collect(Collectors.toList()),
                List.of(10));
    }
}
