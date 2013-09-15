#
# Copyright (c) 2013 Victor Vasiliev
# 
# Python client for Project Athena forum system.
# See LICENSE file for more details.
#

from .rpc import USPBlock, RPCClient, ProtocolError
from . import constants

from functools import total_ordering, wraps
import datetime
import socket

class DiscussError(Exception):
    """An error returned from Discuss server itself which has a Discuss error code."""
    
    def __init__(self, code):
        self.code = code
        
        if code in constants.errors:
            Exception.__init__(self, "Discuss error: %s" % constants.errors[code])
        else:
            Exception.__init__(self, "Unknown discuss error (code %i)" % code)

def autoreconnects(f):
    @wraps(f)
    def autoreconnect(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except socket.timeout:
            self.rpc.connect()
            return f(self, *args, **kwargs)
    return autoreconnect

#
# Here is a practcal description of discuss protocol:
# 1. Connection is established.
# 2. Clients send server a block with a Kerberos ticket. Server is silent.
# 3. Client sends commands. Each command has block type of "command code + 400",
#    each response has to be 0 or an error code.

class Client(object):
    """Discuss client."""

    def __init__(self, server, port = 2100, auth = True, timeout = None):
        self.rpc = RPCClient(server, port, auth, timeout)
        if auth and self.who_am_i().startswith("???@"):
            raise ProtocolError("Authentication to server failed")

    @autoreconnects
    def get_server_version(self):
        """Ask server for the server version number"""

        request = USPBlock(constants.GET_SERVER_VERSION)
        reply = self.rpc.request(request)
        return reply.read_long_integer()

    @autoreconnects
    def who_am_i(self):
        """Ask server for the Kerberos principal with which discuss identified
        the client after the handshake."""

        request = USPBlock(constants.WHO_AM_I)
        reply = self.rpc.request(request)
        return reply.read_string()

    def close(self):
        """Disconnect from the server."""

        self.rpc.socket.close()

class Meeting(object):
    """Discuss meeting."""

    def __init__(self, client, name):
        self.client = client
        self.rpc = client.rpc
        self.name = name
        self.id = (self.rpc.server, name)
        self.info_loaded = False

    @autoreconnects
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

    @autoreconnects
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

    @autoreconnects
    def request_transaction(self, number):
        """Send request for the tranasction into the connection."""

        request = USPBlock(constants.GET_TRN_INFO3)
        request.put_string(self.name)
        request.put_long_integer(number)

        request.block_type += constants.PROC_BASE
        self.rpc.send(request)

    @autoreconnects
    def receive_transaction(self):
        """Read the transaction from the connection."""

        reply = self.rpc.receive()

        version = reply.read_long_integer()
        number = reply.read_long_integer()

        trn = Transaction(self, number)
        trn.version = version
        trn.current = number
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

    @autoreconnects
    def get_transaction(self, number):
        """Retrieve the informataion about a transaction using the number."""

        self.request_transaction(number)

        return self.receive_transaction()

    @autoreconnects
    def transactions(self, start = 1, end = -1, feedback = None):
        """Return an iterator over the given range of transaction. Without
        arguments, iterates over all transactions."""

        if end == -1:
            self.load_info()
            end = self.last

        to_request = end - start + 1
        to_read = to_request
        buffer_size = 500   # Amount of requests which may be sent at one instant
        cur = start

        result = []
        while to_read != 0:
            if to_read - to_request <= buffer_size and to_request > 0:
                # Send another request
                self.request_transaction(cur)
                cur += 1
                to_request -= 1
            else:
                # Start reading things
                try:
                    trn = self.receive_transaction()
                    result.append(trn)
                    if feedback:
                        feedback(cur = trn.number, total = end - start + 1, left = to_read)
                except DiscussError as err:
                    if err.code != constants.DELETED_TRN:
                        raise err
                to_read -= 1

        return result

    @autoreconnects
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

    @autoreconnects
    def get_acl(self):
        """Retrieve the access list of the meeting. Returns the list
        of principal-access tuples."""

        request = USPBlock(constants.GET_ACL)
        request.put_string(self.name)
        reply = self.rpc.request(request)

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        length = reply.read_long_integer()
        acl = []
        for i in range(length):
            modes = reply.read_string()
            principal = reply.read_string()
            # Note: this level of abstraction is probably thinner then I'd like
            acl.append( (principal, modes) )

        return acl

    @autoreconnects
    def get_access(self, principal):
        """Retrieve the access mode of a given Kerberos principal."""

        request = USPBlock(constants.GET_ACCESS)
        request.put_string(self.name)
        request.put_string(principal)
        reply = self.rpc.request(request)

        modes = reply.read_string()
        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

        return modes

    @autoreconnects
    def set_access(self, principal, modes):
        """Changes the access mode of the given principal."""

        request = USPBlock(constants.SET_ACCESS)
        request.put_string(self.name)
        request.put_string(principal)
        request.put_string(modes)
        reply = self.rpc.request(request)

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

    @autoreconnects
    def undelete_transaction(self, trn_number):
        """Undelete the transaction by its number."""

        request = USPBlock(constants.RETRIEVE_TRN)
        request.put_string(self.name)
        request.put_long_integer(trn_number)
        reply = self.rpc.request(request)

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

@total_ordering
class Transaction(object):
    """Discuss transaction. Returned by methods of the meeting object."""

    def __init__(self, meeting, number):
        self.meeting = meeting
        self.number = number
        self.rpc = meeting.rpc

    @autoreconnects
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

    @autoreconnects
    def delete(self):
        """Delete the transaction."""

        request = USPBlock(constants.DELETE_TRN)
        request.put_string(self.meeting.name)
        request.put_long_integer(self.number)
        reply = self.rpc.request(request)

        result = reply.read_long_integer()
        if result != 0:
            raise DiscussError(result)

    def __le__(self, other):
        return self.number < other.number

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self.number == other.number and self.meeting.name == other.meeting.name
        else:
            return False
