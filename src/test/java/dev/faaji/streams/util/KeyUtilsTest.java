package dev.faaji.streams.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyUtilsTest {

    @Test
    void canMergeTwoKeys() {
        String user = "user";
        String key = "key";

        String out = KeyUtils.merge(key, user);
        Assertions.assertEquals("key:user", out);
    }

    @Test
    void canMergeSingleString() {
        String user = "user";
        String out = KeyUtils.merge(user);

        Assertions.assertEquals("user", out);
    }

    @Test void whenMergeEmptyKeys_ThenReturnEmptyString() {
        String out = KeyUtils.merge();

        Assertions.assertEquals("", out);
    }
}