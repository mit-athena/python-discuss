#
# Copyright (c) 2013 Victor Vasiliev
# 
# Python client for Project Athena forum system.
# See LICENSE file for more details.
#

from .rpc import USPBlock, RPCClient, ProtocolError
from . import constants
import datetime

class DiscussError(Exception):
    """An error returned from Discuss server itself which has a Discuss error code."""
    
    def __init__(self, code):
        self.code = code
        
        if code in constants.errors:
            Exception.__init__(self, "Discuss error: %s" % constants.errors[code])
        else:
            Exception.__init__(self, "Unknown discuss error (code %i)" % code)

#
# Here is a practcal description of discuss protocol:
# 1. Connection is establised.
# 2. Clients send server a block with a Kerberos ticket. Server is silent.
# 3. Client sends commands. Each command has block type of "command code + 400",
#    each response has to be 0 or an error code.

class Client(object):
    """Discuss client."""

    def __init__(self, server, port = 2100, auth = True, timeout = None):
        self.rpc = RPCClient(server, port, auth, timeout)
        if auth and self.who_am_i().startswith("???@"):
            raise ProtocolError("Authentication to server failed")

    def get_server_version(self):
        """Ask server for the server version number"""

        request = USPBlock(constants.GET_SERVER_VERSION)
        reply = self.rpc.request(request)
        return reply.read_long_integer()

    def who_am_i(self):
        """Ask server for the Kerberos principal with which discuss identified
        the client after the handshake."""

        request = USPBlock(constants.WHO_AM_I)
        reply = self.rpc.request(request)
        return reply.read_string()

class Meeting(object):
    """Discuss meeting."""

    def __init__(self, client, name):
        self.client = client
        self.rpc = client.rpc
        self.name = name
        self.info_loaded = False

    def load_info(self, force = False):
        """Load all the properties into the class."""

        if self.info_loaded and not force:
            return

        request = USPBlock(constants.GET_MTG_INFO)
        request.put_string(self.name)
        reply = self.rpc.request(request)
        self.version = reply.read_long_integer()
        self.location = reply.read_string()
        self.long_name = reply.read_string()
        self.chairman = reply.read_string()
        self.first = reply.read_long_integer()
        self.last = reply.read_long_integer()
        self.lowest = reply.read_long_integer()
        self.highest = reply.read_long_integer()
        self.date_created = datetime.datetime.fromtimestamp(reply.read_long_integer())
        self.date_modified = datetime.datetime.fromtimestamp(reply.read_long_integer())
        self.public = reply.read_boolean()
        self.access_modes = reply.read_string()

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        self.info_loaded = True

    def check_update(self, last):
        """Check whether the meeting has updated since last time we looked at it.
        Returns true if given last < real last, false if they are equal and error
        if given is greater than real."""

        request = USPBlock(constants.UPDATED_MTG)
        request.put_string(self.name)
        request.put_long_integer(0) # This is the timestamp which server disregards
        request.put_long_integer(last)
        reply = self.rpc.request(request)
        updated = reply.read_boolean()

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        return updated

    def get_transaction(self, number):
        """Retrieve the informataion about a transaction using the number."""

        request = USPBlock(constants.GET_TRN_INFO3)
        request.put_string(self.name)
        request.put_long_integer(number)
        reply = self.rpc.request(request)

        trn = Transaction(self, number)
        trn.version = reply.read_long_integer()
        trn.current = reply.read_long_integer()
        trn.prev = reply.read_long_integer()
        trn.next = reply.read_long_integer()
        trn.pref = reply.read_long_integer()
        trn.nref = reply.read_long_integer()
        trn.fref = reply.read_long_integer()
        trn.lref = reply.read_long_integer()
        trn.chain_index = reply.read_long_integer()
        trn.date_entered = datetime.datetime.fromtimestamp(reply.read_long_integer())
        trn.num_lines = reply.read_long_integer()
        trn.num_chars = reply.read_long_integer()
        trn.subject = reply.read_string()
        trn.author = reply.read_string()
        trn.flags = reply.read_long_integer()
        trn.signature = reply.read_string()

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        return trn

    def transactions(self, start = 1, end = -1):
        """Return an iterator over the given range of transaction. Without
        arguments, iterates over all transactions."""

        if end == -1:
            self.load_info()
            end = self.last

        next = start
        while next <= end and next != 0:
            try:
                trn = self.get_transaction(next)
                yield trn
                next = trn.next
            except DiscussError as err:
                if err.code == constants.DELETED_TRN:
                    next += 1
                else:
                    raise err

    def post(self, text, subject, signature = None, reply_to = 0):
        """Add a transaction to the meeting."""

        request = USPBlock(constants.PROC_BASE + (constants.ADD_TRN2 if signature else constants.ADD_TRN))
        request.put_string(self.name)
        request.put_long_integer(len(text))
        request.put_string(subject)
        if signature:
            request.put_string(signature)
        request.put_long_integer(reply_to)

        # Yes, there is no two-byte padding involved.  I was actually
        # surprised. It is quite possible that this is actually broken in some
        # clever way.
        tfile = USPBlock(constants.TFILE_BLK)
        tfile.buffer = text

        self.rpc.send(request)
        self.rpc.send(tfile)
        reply = self.rpc.receive()
        if reply.block_type != constants.REPLY_TYPE:
            raise ProtocolError("Transport-level error")
        
        new_id = reply.read_long_integer()
        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        return self.get_transaction(new_id)

class Transaction(object):
    """Discuss transaction. Returned by methods of the meeting object."""

    def __init__(self, meeting, number):
        self.meeting = meeting
        self.number = number
        self.rpc = meeting.rpc

    def get_text(self):
        """Retrieve the text of the transaction."""

        request = USPBlock(constants.PROC_BASE + constants.GET_TRN)
        request.put_string(self.meeting.name)
        request.put_long_integer(self.number)
        request.put_long_integer(0)
        self.rpc.send(request)

        tfile = self.rpc.receive()
        reply = self.rpc.receive()
        if tfile.block_type != constants.TFILE_BLK or reply.block_type != constants.REPLY_TYPE:
            raise ProtocolError("Bad server response when retriving transaction contents")
        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        return tfile.buffer
