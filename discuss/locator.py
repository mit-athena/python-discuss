#
# Python client for Project Athena forum system.
# See LICENSE file for more details.
#
# The following file contains routine used to locate
# a meeting my its name using the two sources for location
# lists: /etc/discuss/servers and ~/.dsc_servers. Both
# of those files are formatted as lists of hostnames
# with both empty lines and comments prefixed by "#" sign.
#

from .client import Client, Meeting, DiscussError
from .constants import NO_SUCH_MTG
from functools import partial
import errno
import os
import re

def _read_server_list(filename):
    """Parses the given list of discuss servers."""

    try:
        source = open(filename, "r")
        lines = source.readlines()
        source.close()

        remove_comments = partial(re.sub, "#.*", "")

        lines = map(remove_comments, lines)    # comments
        lines = map(str.strip, lines)          # whitespace
        lines = [x for x in lines if x]     # empty lines

        return lines
    except IOError as err:
        # File is allowed not to exist
        if err.errno == errno.ENOENT:
            return []
        else:
            raise err

def get_servers():
    global_list_path = "/etc/discuss/servers"
    user_list_path = os.path.expanduser("~/.dsc_servers")

    global_servers = _read_server_list(global_list_path)
    user_servers  = _read_server_list(user_list_path)

    return global_servers + [ server
            for server in user_servers if server not in global_servers ]

def locate(name):
    """Attempts to locate the meeting by looking for it on known
    discuss servers. If found, returns the meeting object with a live
    connection."""

    servers = get_servers()
    for server in servers:
        client = Client(server)
        for prefix in ("/var/spool/discuss/", "/usr/spool/discuss/"):
            mtg_path = prefix + name
            mtg = Meeting(client, mtg_path)
            try:
                mtg.load_info()
                return mtg
            except DiscussError as err:
                if err.code == NO_SUCH_MTG:
                    continue
                else:
                    client.close()
                    raise err

        client.close()

    return None
