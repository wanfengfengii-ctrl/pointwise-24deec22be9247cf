"""Finite Modbus/TCP profile: unit 1, function 3, one uint16 register."""
import asyncio
import struct

async def read_adu(reader):
    head = await reader.readexactly(7)
    tid, protocol, length, unit = struct.unpack(">HHHB", head)
    if protocol != 0 or unit != 1 or not 2 <= length <= 254:
        raise ValueError("invalid MBAP")
    return tid, await reader.readexactly(length - 1)

def adu(tid, pdu):
    return struct.pack(">HHHB", tid, 0, len(pdu) + 1, 1) + pdu

def request(tid, address):
    return adu(tid, struct.pack(">BHH", 3, address, 1))

def request_address(pdu):
    if len(pdu) != 5:
        raise ValueError("invalid request length")
    function, address, count = struct.unpack(">BHH", pdu)
    if function != 3 or count != 1 or address not in (0, 1):
        raise ValueError("unsupported request")
    return address

def response(tid, value=None, error=None):
    if error is not None:
        return adu(tid, bytes((0x83, error)))
    return adu(tid, struct.pack(">BBH", 3, 2, value))

def response_value(pdu):
    if len(pdu) == 2 and pdu[0] == 0x83:
        raise ValueError("device exception " + str(pdu[1]))
    if len(pdu) != 4 or pdu[:2] != bytes((3, 2)):
        raise ValueError("invalid read response")
    return struct.unpack(">H", pdu[2:])[0]
