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

class Client(object):
    def __init__(self, server, port = 2100, auth = True, timeout = None):
        self.rpc = RPCClient(server, port, auth, timeout)
        if auth and self.who_am_i().startswith("???@"):
            raise ProtocolError("Authentication to server failed")

    def get_server_version(self):
        request = USPBlock(constants.GET_SERVER_VERSION)
        reply = self.rpc.request(request)
        return reply.read_long_integer()

    def who_am_i(self):
        request = USPBlock(constants.WHO_AM_I)
        reply = self.rpc.request(request)
        return reply.read_string()

class Meeting(object):
    def __init__(self, client, name):
        self.client = client
        self.rpc = client.rpc
        self.name = name
        self.info_loaded = False

    def load_info(self, force = False):
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

    def get_transaction(self, number):
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

class Transaction(object):
    def __init__(self, meeting, number):
        self.meeting = meeting
        self.number = number
        self.rpc = meeting.rpc

    def get_text(self):
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
