#
# Copyright (c) 2013 Victor Vasiliev
# 
# Python client for Project Athena forum system.
# See LICENSE file for more details.
#
# I give absolutely no guranatees that this code will work and that it will
# not do anything which may lead to accidental corruption or destruction of
# the data. As you will read the file, you will see why.
#

# 
# This file implements the protocol for discuss. discuss is the forum service
# from Project Athena which was intended as a clone of Multics forum
# application. discuss(1) still refers you to "Multics forum manual" (with a
# smileyface, which is probably due to the fact that the manpage itself hardly
# fills half of the normal terminal window size).
#
# By 2013, when this comment was written, that forum was used solely for
# storing mailing list archives. Hence this implementation at the current
# moment is sufficient only for extracting discussions, but not for posting.
#
# The protocol itself is based upon USP: "UNIX Universal Streaming Protocol",
# which was apparently one of the attempts to create a universal data
# representation protocol (like XDR, ASN.1, XML, JSON, protobufs, etc) used by
# the discuss developers because that was a new shiny thing from LCS back in
# the day. One would guess that since the only implementation of it still in
# the wild is discuss, the protocol is only used by discuss itself. This is,
# not, however, true. Discuss does not actually use USP: it hijacks into the
# middle of USP library, copies the parts of the connection code and then uses
# the USP data representation routines (which are not even exported from that
# library in heaeder files) without actually doing USP.
#
# As I found out (because of the copyright header), the protocol was part of
# certain distriubted mail system called PCmail, which even has a few RFCs
# dedicated to it.
#
# The USP code is in usp/ tree and the discuss usage of it is in
# libds/rpcall.c. Note that in Debathena those are compiled as two different
# static libraries. libds uses usp routines, even though they are not even
# exported in the header file. On, and the whole suite is written in K&R C.
#

import errno
import socket
from struct import pack, unpack, calcsize
from functools import partial

from . import constants

class ProtocolError(Exception):
    pass

# Data formats, in their USP names. USP "cardinal" means "unsigned" or something
# like that (discuss rpcall.c calls it "short", which is more reasonable).
_formats = {
    "boolean" : "!H",   # Yes, really, bool is two bytes
    "integer" : "!h",
    "cardinal" : "!H",
    "long_integer" : "!i",
    "long_cardinal" : "!I",
}

# This is a horrible kludge which I wrote for pymoira and hoped to forget that
# it exists and that I ever wrote it. Unfortunately, it looks like Moira is not
# the only Athena service which totally disregards such nice thing like GSSAPI.
def _get_krb5_ap_req(service, server):
    """Returns the AP_REQ Kerberos 5 ticket for a given service."""

    import kerberos, base64
    try:
        status_code, context = kerberos.authGSSClientInit( '%s@%s' % (service,server) )
        kerberos.authGSSClientStep(context, "")
        token_gssapi = base64.b64decode( kerberos.authGSSClientResponse(context) )

        # The following code "parses" GSSAPI token as described in RFC 2743 and
        # RFC 4121.  "Parsing" in this context means throwing out the GSSAPI
        # header (because YOLO/IBTSOCS) while doing some very basic validation
        # of whether this is actually what we want.
        # 
        # This code is here because Python's interface provides only GSSAPI
        # interface, and discuss does not use GSSAPI. This should be fixed at
        # some point, hopefully through total deprecation of discuss. Thermite
        # involvement is preferred.
        # 
        # FIXME: this probably should either parse tokens properly or use
        # another Kerberos bindings for Python. Currently there are no sane
        # Python bindings for krb5 I am aware of. There's krb5 module, which
        # has not only terrible API, but also confusing error messages and
        # useless documentation. Perhaps the only fix is to write proper
        # bindings myself, but this is the yak I am not ready to shave at the
        # moment.
        
        body_start = token_gssapi.find( chr(0x01) + chr(0x00) )    # 01 00 indicates that this is AP_REQ
        if token_gssapi[0] != chr(0x60) or \
        not (token_gssapi[2] == chr(0x06) or token_gssapi[4] == chr(0x06)) or \
        body_start == -1 or body_start < 8 or body_start > 64:
            raise ProtocolError("Invalid GSSAPI token provided by Python's Kerberos API")

        body = token_gssapi[body_start + 2:]
        return body
    except kerberos.GSSError as err:
        raise ProtocolError("Kerberos authentication error: %s" % err[1][0])

class USPBlock(object):
    """Class which allows to build USP blocks."""

    def __init__(self, block_type):
        # Create read_* and put_* methods
        self.__dict__.update({
                ("put_" + name) : partial(self.put_data, fmt)
                for name, fmt in _formats.items()
            })
        self.__dict__.update({
                ("read_" + name) : partial(self.read_data, fmt)
                for name, fmt in _formats.items()
            })

        self.buffer = b""
        self.block_type = block_type

    def put_data(self, fmt, s):
        """Put formatted data into the buffer."""

        self.buffer += pack(fmt, s)

    def put_string(self, s):
        """Put a string into the buffer."""

        if "\0" in s:
            raise USPError("Null characeters are not allowed in USP")

        # "\n" is translated to "\r\n", and "\r" to "\r\0". Because we can. Or
        # because that seemed like a nice cross-platform feature. Or for weird
        # technical reasons from 1980s I do not really want to know. This works
        # out because input is null-terminated and wire format is has length
        # specified.
        encoded = s.replace("\r", "\r\0").replace("\n", "\r\n")
        self.put_cardinal(len(encoded))
        self.buffer += encoded

        # Padding
        if len(encoded) % 2 == 1:
            self.buffer += "\0"

    def send(self, sock):
        """Sends the block over a socket."""

        # Maximum size of a subblock (MAX_SUB_BLOCK_LENGTH)
        magic_number = 508

        sock.sendall(pack("!H", self.block_type))

        # Each block is fragmented into subblocks with a 16-bit header
        unsent = self.buffer
        first_pass = True
        while len(unsent) > 0 or first_pass:
            first_pass = False

            if len(unsent) > magic_number:
                current, unsent = unsent[0:magic_number], unsent[magic_number:]
                last = False
            else:
                current, unsent = unsent, ""
                last = True

            # Header is length of the subblock + last block marker
            header_number = len(current) + 2   # Length + header size
            if last: 
                header_number |= 0x8000
            header = pack("!H", header_number)

            sock.sendall(header + current)

    def read_data(self, fmt):
        """Read a data using a type specifier."""

        size = calcsize(fmt)
        if len(self.buffer) < size:
            raise ProtocolError("Invalid data received from the client (block is too short)")
        
        data, self.buffer = self.buffer[0:size], self.buffer[size:]
        unpacked, = unpack(fmt, data)
        return unpacked

    def read_string(self):
        """Read a string from the buffer."""

        size = self.read_cardinal()

        if len(self.buffer) < size:
            raise ProtocolError("Invalid data received from the client (block is too short)")
        omit = size + 1 if size % 2 ==1 else size  # due to padding
        encoded, self.buffer = self.buffer[0:size], self.buffer[omit:]

        return encoded.replace("\r\n", "\n").replace("\r\0", "\r")

    @staticmethod
    def receive(sock):
        """Receives a block sent over the network."""

        header = sock.recv(2)
        block_type, = unpack("!H", header)
        block = USPBlock(block_type)

        # Note that here I deliberately increase the size compared to send()
        # because some of the code suggests that blocks larger than 512 bytes
        # may actually exist
        magic_number = 4096

        last = False
        while not last:
            subheader, = unpack("!H", sock.recv(2))
            last = (subheader & 0x8000) != 0
            size = (subheader & 0x0FFF) - 2
            if size > magic_number:
                raise ProtocolError("Subblock size is too large")

            buffer = b""
            while len(buffer) < size:
                old_len = len(buffer)
                buffer += sock.recv(size - len(buffer))
                if len(buffer) == old_len:
                    raise ProtocolError("Connection broken while transmitting a block")

            block.buffer += buffer

        return block

class RPCClient(object):
    def __init__(self, server, port, auth = True, timeout = None):
        self.server = socket.getfqdn(server).lower()
        self.port = port
        self.auth = auth
        self.timeout = timeout

        self.connect()
        self.make_wrapper()

    def connect(self):
        self.socket = socket.create_connection((self.server, self.port), self.timeout)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if not hasattr(self, 'wrapper'):
            self.wrapper = self.socket

        auth_block = USPBlock(constants.KRB_TICKET)
        if self.auth:
            authenticator = _get_krb5_ap_req( "discuss", self.server )

            # Discuss does the same thing for authentication as Moira does: it
            # sends AP_REQ to the server and prays that we do not get MITMed,
            # and that Kerberos will protect us from possible replay attacks on
            # that and what else. In Moira it was disappointing given that
            # GSSAPI exists for ~20 years and Moira was reasonably maintained
            # in general. I'm not judging discuss much, because it did not
            # receive much care since it was originally developed.
            # 
            # What fascinates me here is the way discuss decided to improve on
            # the Moira's authentication protocol. Instead of just sending the
            # Kerberos ticket, it represents it as an array of bytes, and then
            # it takes every byte and converts it into a network-order short.
            #
            # My current hypothesis is that this is because USP does not
            # support bytes and sending things as an array of shorts seemed
            # like the easiest way to use the underlying buffer-control
            # routines.
            #
            # You may bemoan the state of computer science, but looking at
            # this, I feel like we became better at protocol design over last
            # 20 years.

            auth_block.put_cardinal(len(authenticator))
            for byte in authenticator:
                auth_block.put_cardinal(ord(byte))
        else:
            auth_block.put_cardinal(0)

        self.send(auth_block)

    def make_wrapper(self):
        class SocketWrapper(object):
            def recv(self2, *args, **kwargs):
                try:
                    return self.socket.recv(*args, **kwargs)
                except socket.error as err:
                    if err.errno == errno.EINTR:
                        return self2.recv(*args, **kwargs)
                    else:
                        raise err

            def sendall(self2, *args, **kwargs):
                try:
                    return self.socket.sendall(*args, **kwargs)
                except socket.error as err:
                    if err.errno == errno.EINTR:
                        return self2.sendall(*args, **kwargs)
                    else:
                        raise err

        self.wrapper = SocketWrapper()

    def send(self, block):
        block.send(self.wrapper)

    def receive(self):
        return USPBlock.receive(self.wrapper)

    def request(self, block):
        block.block_type += constants.PROC_BASE
        self.send(block)
        reply = self.receive()
        if reply.block_type != constants.REPLY_TYPE:
            raise ProtocolError("Transport-level error")
        return reply

