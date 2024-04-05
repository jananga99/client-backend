from base64 import b64encode, b64decode


def to_str_chunk(chunk):
    return b64encode(chunk).decode("utf-8")


def to_byte_chunk(chunk_data):
    return b64decode(chunk_data)
