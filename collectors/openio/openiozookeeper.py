# coding=utf-8

"""
Collect zookeeper stats. ( Modified from memcached collector )

#### Dependencies

 * subprocess
 * Zookeeper 'mntr' command (zookeeper version => 3.4.0)

#### Example Configuration

OpenioZookeeperCollector.conf

```
    enabled = True
    hosts = localhost:2181, app-1@localhost:2181, app-2@localhost:2181, etc
```

TO use a unix socket, set a host string like this

```
    hosts = /path/to/blah.sock, app-1@/path/to/bleh.sock,
```
"""

import diamond.collector
import socket
import re


class OpenioZookeeperCollector(diamond.collector.Collector):

    def process_config(self):
        super(OpenioZookeeperCollector, self).process_config()
        self.instances = self.config['instances'] or 'OPENIO:localhost:6005'

    def get_default_config_help(self):
        config_help = super(OpenioZookeeperCollector, self).get_default_config_help()
        config_help.update({
            'publish':
                "Which rows of 'status' you would like to publish." +
                " Telnet host port' and type stats and hit enter to see the " +
                " list of possibilities. Leave unset to publish all.",
            'instances':
                "List of namespaces, hosts, and ports to collect.",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(OpenioZookeeperCollector, self).get_default_config()
        config.update({
            'path':     'openio',

            # Which rows of 'status' you would like to publish.
            # 'telnet host port' and type mntr and hit enter to see the list of
            # possibilities.
            # Leave unset to publish all
            # 'publish': ''

            # Connection settings
            #'instances': ['OPENIO:localhost:6005'],
        })
        return config

    def get_raw_stats(self, host, port):
        data = ''
        # connect
        try:
            if port is None:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(host)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, int(port)))
            # request stats
            sock.send('mntr\n')
            # something big enough to get whatever is sent back
            data = sock.recv(4096)
        except socket.error:
            self.log.exception('Failed to get stats from %s:%s',
                               host, port)
        return data

    def _get_stats(self, host, port):
        # stuff that's always ignored, aren't 'stats'
        ignored = ('zk_version', 'zk_server_state')
        pid = None

        stats = {}
        data = self.get_raw_stats(host, port)

        # parse stats
        for line in data.splitlines():

            pieces = line.split()

            if pieces[0] in ignored:
                continue
            stats[pieces[0]] = pieces[1]

        # get max connection limit
        self.log.debug('pid %s', pid)
        try:
            cmdline = "/proc/%s/cmdline" % pid
            f = open(cmdline, 'r')
            m = re.search("-c\x00(\d+)", f.readline())
            if m is not None:
                self.log.debug('limit connections %s', m.group(1))
                stats['limit_maxconn'] = m.group(1)
            f.close()
        except:
            self.log.debug("Cannot parse command line options for zookeeper")

        return stats

    def collect(self):
        instances = self.config.get('instances')

        # Convert a string config value to be an array
        if isinstance(instances , basestring):
            instances = instances.split(',')

        for instance in instances:
            namespace, hostname, port = instance.split(':')

            stats = self._get_stats(hostname, port)

            metric_prefix = '%s.zookeeper.%s:%s.' % (namespace,
                                                     hostname.replace('.', '_'),
                                                     port)

            # figure out what we're configured to get, defaulting to everything
            desired = self.config.get('publish', stats.keys())

            # for everything we want
            for stat in desired:
                if stat in stats:
                    self.publish(metric_prefix + stat, stats[stat])
                else:

                    # we don't, must be somehting configured in publish so we
                    # should log an error about it
                    self.log.error("No such key '%s' available, issue 'stats' "
                                   "for a full list", stat)
