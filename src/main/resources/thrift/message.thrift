namespace java com.leansoft.luxun.message.generated
namespace csharp Leansoft.luxun.message.Generated
/**
 * FOR INTERNAL USE, DO NOT CHANGE! 
 */
struct TMessageList {
    1: list<binary> messages
}

/**
 * FOR INTERNAL USE, DO NOT CHANGE! 
 *
 * A package of a list of messages. The format of an N bytes package is the following:
 * 
 * current magic byte is 1
 *
 * 1. 1 byte "magic" identifier to allow format changes
 * 2. 1 byte "attribute" identifier to allow annotations on the package
 *      independent of the version (e.g. compression enabled, type of codec used)
 * 3. 4 bytes CRC32 of the payload
 * 4. N bytes messageList, thrift serialized MessageList, may be compressed according to attribute setting
 */
struct TMessagePack {
    1: byte magic = 1,
    2: byte attribute,
    3: i32 crc32,
    4: required binary messageList
}


enum CompressionCodec
{
  NO_COMPRESSION,
  GZIP,
  SNAPPY
}