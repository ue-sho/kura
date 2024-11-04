import struct

INT32_BYTES: i32 = 4
UTF16_SIZE: i32 = 2


class Page:
    buffer: Array[byte]

    def __init__(self, block_size: i64):
        """
        Initializes the Page with a new buffer of specified block size.

        Args:
            block_size (i64): The size of the buffer in bytes.
        """
        self.buffer = Array[byte](block_size)

    def __init__(self, buffer: Array[byte]):
        """
        Initializes the Page with an existing buffer.
        ※ Codon supports method overloading.

        Args:
            buffer (Array[byte]): The byte array to be used as the buffer.
        """
        self.buffer = buffer

    def get_int(self, offset: i32) -> i32:
        """
        Retrieves a 32-bit integer from the buffer at the specified offset.

        Args:
            offset (i32): The position within the buffer to read the integer from.

        Returns:
            i32: The 32-bit integer read from the specified offset.
        """
        # '<' indicates little-endian byte order and 'i' indicates 32-bit integer
        return struct.unpack_from("<i", self.buffer, offset)[0]

    def set_int(self, offset: i32, val: i32):
        """
        Sets a 32-bit integer at the specified offset in the buffer.

        Args:
            offset (i32): The position within the buffer to write the integer to.
            val (i32): The 32-bit integer value to be written.
        """
        struct.pack_into("<i", self.buffer, offset, val)

    def get_bytes(self, offset: i32) -> bytes:
        """
        Retrieves a byte sequence from the buffer starting at the specified offset.
        The first 4 bytes indicate the length of the byte sequence.

        Args:
            offset (i32): The position within the buffer to read the byte sequence from.

        Returns:
            bytes: The byte sequence retrieved from the buffer.
        """
        length = self.get_int(offset)
        return self.buffer[offset + INT32_BYTES : offset + INT32_BYTES + length]

    def set_bytes(self, offset: i32, val: bytes):
        """
        Sets a byte sequence at the specified offset in the buffer, storing the length first.

        Args:
            offset (i32): The position within the buffer to write the byte sequence to.
            val (bytes): The byte sequence to be stored in the buffer.
        """
        self.set_int(offset, len(val))
        self.buffer[offset + INT32_BYTES : offset + INT32_BYTES + len(val)] = val

    def get_string(self, offset: i32) -> str:
        """
        Retrieves a UTF-16 encoded string from the buffer starting at the specified offset.
        The first 4 bytes indicate the length of the string in bytes.

        Args:
            offset (i32): The position within the buffer to read the string from.

        Returns:
            str: The decoded UTF-16 string.
        """
        length = self.get_int(offset) // UTF16_SIZE
        runes = [
            self.get_uint16(offset + INT32_BYTES + i * UTF16_SIZE)
            for i in range(length)
        ]
        return "".join(chr(r) for r in runes)

    def set_string(self, offset: i32, val: str):
        """
        Sets a UTF-16 encoded string at the specified offset in the buffer.

        Args:
            offset (i32): The position within the buffer to write the string to.
            val (str): The string to be encoded and stored in the buffer.
        """
        runes = [ord(char) for char in val]
        self.set_int(offset, len(runes) * UTF16_SIZE)
        for i, r in enumerate(runes):
            self.set_uint16(offset + INT32_BYTES + i * UTF16_SIZE, r)

    def get_uint16(self, offset: i32) -> u16:
        """
        Retrieves a 16-bit unsigned integer from the buffer at the specified offset.

        Args:
            offset (i32): The position within the buffer to read the integer from.

        Returns:
            u16: The 16-bit unsigned integer read from the specified offset.
        """
        return struct.unpack_from("<H", self.buffer, offset)[0]

    def set_uint16(self, offset: i32, val: u16):
        """
        Sets a 16-bit unsigned integer at the specified offset in the buffer.

        Args:
            offset (i32): The position within the buffer to write the integer to.
            val (u16): The 16-bit unsigned integer value to be written.
        """
        struct.pack_into("<H", self.buffer, offset, val)

    @staticmethod
    def max_length(length: i32) -> i32:
        """
        Calculates the maximum buffer size needed to store a UTF-16 string of a given length.

        Args:
            length (i32): The number of characters in the string.

        Returns:
            i32: The maximum buffer size in bytes to store the string.
        """
        return INT32_BYTES + length * UTF16_SIZE
