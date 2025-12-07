
package net.quanzy.authzmod.db

import java.nio.ByteBuffer

/**
 * Generic serializer interface for serializing and deserializing values.
 */
sealed interface Serializers<KEY> {
    fun serialize(value: KEY): ByteBuffer
    fun read(buffer: ByteBuffer): KEY
}

@Suppress("UNCHECKED_CAST")
fun<K> findSerializerForClass(klazz: Class<K>): Serializers<K> {
    return when (klazz) {
        String::class.java -> StringSerializer() as Serializers<K>
        else -> throw IllegalArgumentException("No serializer found for class: ${klazz.name}")
    }
}

/**
 * String serializer implementation.
 */
class StringSerializer: Serializers<String> {
    override fun read(buffer: ByteBuffer): String {
        val size = buffer.int
        val bytes = ByteArray(size)
        buffer.get(bytes)
        return String(bytes)
    }

    override fun serialize(value: String): ByteBuffer {
        val bytes = value.toByteArray()
        val buffer = ByteBuffer.allocate(Integer.BYTES + bytes.size)
        buffer.putInt(bytes.size)
        buffer.put(bytes)
        buffer.flip()
        return buffer
    }
}