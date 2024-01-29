"""jc - JSON Convert `unbound-control` command output parser

Usage (cli):

    $ unbound-control | jc --unbound-control

or

    $ jc unbound-control

Usage (module):

    import jc
    result = jc.parse('unbound_control', unbound_control_command_output)

Schema:

    [
      {
        "version":          string,
        "verbosity":        integer,
        "threads":          integer,
        "modules":          string,
        "uptime":           integer,
        "options":          strings,
        "unbound (pid)":    integer
      }
    ]

Examples:

    $ unbound-control | jc --unbound-control status
    [
      {
        "version": 1.17.1,
        "verbosity": 1,
        "threads": 4,
        "modules": "4 [ validator iterator ]",
        "uptime": 435248
        "options": "reuseport control(ssl)",
        "pid": 3699276
      }
    ]


"""
from typing import List, Dict
import jc.utils


class info():
    """Provides parser metadata (version, author, etc.)"""
    version = '1.0'
    description = '`unbound-control` command parser'
    author = 'Pettai'
    author_email = 'pettai@sunet.se'
    compatible = ['linux', 'darwin', 'cygwin', 'win32', 'aix', 'freebsd']
    tags = ['command']
    magic_commands = ['unbound-control']


__version__ = info.version


def _process(proc_data):
    """
    Final processing to conform to the schema.

    Parameters:

        proc_data:   (List of Dictionaries) raw structured data to process

    Returns:

        List of Dictionaries. Structured to conform to the schema.
    """
    int_list = {'verbosity', 'threads', 'uptime', 'pid'}

    for entry in proc_data:
        for key in entry:
            if key in int_list:
                entry[key] = jc.utils.convert_to_int(entry[key])

    return proc_data


def parse(data: str, raw: bool = False, quiet: bool = False):
    """
    Main text parsing function

    Parameters:

        data:        (string)  text data to parse
        raw:         (boolean) unprocessed output if True
        quiet:       (boolean) suppress warning messages if True

    Returns:

        List of Dictionaries. Raw or processed structured data.
    """
    jc.utils.compatibility(__name__, info.compatible, quiet)
    jc.utils.input_type_check(data)

    raw_output: List[Dict] = []

    if jc.utils.has_data(data):

        itrparse = False
        itr: Dict = {}

        for line in filter(None, data.splitlines()):
            line = line.strip()

            # default 'ok'
            if line.startswith('ok'):
                raw_output.append({'command': 'ok'})
                continue

            # status
            if line.startswith('version:'):
                status = {}
                linedata = line.split(':', maxsplit=1)
                version = linedata[1].strip()
                status.update({'version': version})
                continue

            if line.startswith('verbosity:'):
                linedata = line.split(':', maxsplit=1)
                verbosity = linedata[1]
                status.update({'verbosity': verbosity})
                continue

            if line.startswith('threads:'):
                linedata = line.split(': ', maxsplit=1)
                threads = linedata[1]
                status.update({'threads': threads})
                continue

            if line.startswith('modules:'):
                linedata = line.split(': ', maxsplit=1)
                modules = linedata[1]
                status.update({'modules': modules})
                continue

            if line.startswith('uptime:'):
                linedata = line.split(': ', maxsplit=1)
                uptime = linedata[1].strip(' seconds')
                status.update({'uptime': uptime})
                continue

            if line.startswith('options:'):
                linedata = line.split(': ', maxsplit=1)
                options = linedata[1]
                status.update({'options': options})
                continue

            if line.startswith('unbound'):
                linedata = line.split(' ', maxsplit=4)
                pid = linedata[2].strip(')')
                status.update({'pid': pid})
                raw_output.append(status)
                continue


            # list_xxxx_zones
            if line.endswith(' static'):
                itrparse = True
                linedata = line.split(' ', maxsplit=1)
                zone = linedata[0]
                itr.update({zone: 'static'})
                continue

            if line.endswith(' redirect'):
                itrparse = True
                linedata = line.split(' ', maxsplit=1)
                zone = linedata[0]
                itr.update({zone: 'redirect'})
                continue

            if "serial " in line:
                itrparse = True
                linedata = line.split('\t', maxsplit=2)
                zone = linedata[0]
                serial = int(linedata[1].strip('serial '))
                itr.update({zone: serial})
                continue


            # stats
            if line.startswith('thread') or line.startswith('total.') or line.startswith('time.') or line.startswith('mem.') or line.startswith('num.') or line.startswith('unwanted.') or line.startswith('msg.') or line.startswith('rrset.') or line.startswith('infra.') or line.startswith('key.'):
                itrparse = True
                linedata = line.split('=', maxsplit=1)
                key = linedata[0]
                if key.startswith('time.') or key.endswith('.recursion.time.avg') or key.endswith('.requestlist.avg') or key.endswith('.recursion.time.median'):
                    value = float(linedata[1])
                else:
                    value = int(linedata[1])
                itr.update({key: value})
                continue

        if itrparse:
            raw_output.append(itr)

    return raw_output if raw else _process(raw_output)
