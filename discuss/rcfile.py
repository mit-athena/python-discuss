#
# Copyright (c) 2013 Victor Vasiliev
# 
# Python client for Project Athena forum system.
# See LICENSE file for more details.
#

import os, errno, re, time

def locate_rc_file():
    """Determine the location of .meetings file."""

    # This behavior is sort of based on libds behavior
    if "MEETINGS" in os.environ:
        return os.environ["MEETINGS"]

    return os.path.expanduser("~/.meetings")

def get_default_meetings():
    """Determine the default meetings if .meetings file does not exist."""

    try:
        source = open("/etc/discuss/meetings.default", "r")
        default = source.read()
        source.close()
        return default
    except IOError as err:
        # File is allowed not to exist
        if err.errno == errno.ENOENT:
            return ""
        else:
            raise err

class RCFile:
    """The .meetings file interface."""

    def __init__(self, location = None):
        if not location:
            location = locate_rc_file()

        self.location = location

        if not os.path.isfile(location):
            default = get_default_meetings()
            self.updateContents(default)

        self.load()

    def updateContents(self, text):
        """Update the contents of .meetings file with the following text."""

        text = text.strip() # Trailing newlines break things in our case
        rcfile = open(self.location, "w")
        rcfile.write(text)
        rcfile.close()

    def load(self):
        """Read all the entries in the .meetings file into the object."""

        rcfile = open(self.location, "r")
        entries = {}
        for line in rcfile:
            match = re.match(r"^(\d):(\d+):(\d+):([a-zA-Z\d.\-]+):([^:]+):([^:]+):$", line.strip())
            if not match:
                raise ValueError("Malformed .meetings file entry: '%s'" % (line.strip(),))
            status = int(match.group(1))
            entry = {
                'changed' : bool(status & 0x01),
                'deleted' : bool(status & 0x02),
                'last_timestamp' : int(match.group(2)),
                'last_transaction' : int(match.group(3)),
                'hostname' : match.group(4).lower(),
                'path' : match.group(5),
                'names' : match.group(6).split(','),
            }

            # Convenience variables
            entry['displayname'] = entry['path'].split('/')[-1]
            entry_id = (entry['hostname'], entry['path'])
            entry['location'] = '%s:%s' % entry_id
            entries[entry_id] = entry

        self.entries = entries
        self.recache()

    def recache(self):
        """Update the meeting name lookup cache."""

        self.cache = {}
        for entry in self.entries.values():
            for name in entry['names']:
                self.cache[name] = (entry['hostname'], entry['path'])
            self.cache[entry['location']] = (entry['hostname'], entry['path'])

    def save(self):
        """Save the new .meetings file."""

        rcfile = open(self.location, "w")
        for entry in self.entries.values():
            status = 0x00
            if entry['changed']: status |= 0x01
            if entry['deleted']: status |= 0x02

            line = "%d:%d:%d:%s:%s:%s:\n" % (status, entry['last_timestamp'],
                    entry['last_transaction'], entry['hostname'], entry['path'],
                    ','.join(entry['names']))
            rcfile.write(line)

        rcfile.close()

    def lookup(self, name):
        """Look up the meeting name and get a (host, path) tuple."""

        if type(name) == tuple:
            return name

        return self.cache.get(name)

    def touch(self, meeting, last):
        """Set the last read entry for given meeting."""

        meeting = self.lookup(meeting)
        if meeting not in self.entries:
            raise ValueError("Attempted to touch the non-existent meeting")

        self.entries[meeting]['last_timestamp'] = int(time.time())
        self.entries[meeting]['last_transaction'] = int(last)

    def add(self, meeting):
        """Adds a given meeting object to .meetings file."""

        mtg_id = meeting.id
        if mtg_id in self.entries:
            raise ValueError("Meeting %s:%s is already in .meetings" % mtg_id)

        meeting.load_info()
        displayname = mtg_id[1].split('/')[-1]
        if self.lookup(displayname):
            raise ValueError("Meeting %s is already in .meetings" % displayname)
        if self.lookup(meeting.long_name):
            raise ValueError("Meeting %s is already in .meetings" % meeting.long_name)

        entry = {
            'changed' : True,
            'deleted' : False,
            'last_timestamp' : 0,
            'last_transaction' : 1,
            'hostname' : mtg_id[0],
            'path' : mtg_id[1],
            'names' : [meeting.long_name, displayname],
            'displayname' : displayname,
            'location' : '%s:%s' % mtg_id,
        }
        self.entries[mtg_id] = entry
        self.recache()

