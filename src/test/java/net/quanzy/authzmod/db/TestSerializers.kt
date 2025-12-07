package net.quanzy.authzmod.db

import org.junit.jupiter.api.Test


class TestSerializers {
    @Test
    fun testStringSerializer() {
        val serializer = StringSerializer()
        val originalString = "andrew"
        val serialized = serializer.serialize(originalString)
        val deserialized = serializer.read(serialized)
        assert(originalString == deserialized)
    }
}
