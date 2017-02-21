# coding=utf-8

"""
Collects the following from beanstalkd:
    - Server statistics via the 'stats' command
    - Per tube statistics via the 'stats-tube' command

#### Dependencies

 * beanstalkc

"""

import re
import diamond.collector

try:
    import beanstalkc
except ImportError:
    beanstalkc = None


class OpenioBeanstalkdCollector(diamond.collector.Collector):
    SKIP_LIST = ['version', 'id', 'hostname']
    COUNTERS_REGEX = re.compile(
        r'^(cmd-.*|job-timeouts|total-jobs|total-connections)$')

    def process_config(self):
        super(OpenioBeanstalkdCollector, self).process_config()
        self.instances = self.config['instances'] or 'OPENIO:127.0.0.1:6014'

    def get_default_config_help(self):
        config_help = super(OpenioBeanstalkdCollector,
                            self).get_default_config_help()
        config_help.update({
            'instances': 'List of instances in the form NAMESPACE:host:port (comma separated)',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(OpenioBeanstalkdCollector, self).get_default_config()
        config.update({
            'path':     'openio',
        })
        return config

    def _get_stats(self,host,port):
        stats = {}
        try:
            connection = beanstalkc.Connection(host,int(port))
        except beanstalkc.BeanstalkcException, e:
            self.log.error("Couldn't connect to beanstalkd: %s", e)
            return {}

        stats['instance'] = connection.stats()
        stats['tubes'] = []

        for tube in connection.tubes():
            tube_stats = connection.stats_tube(tube)
            stats['tubes'].append(tube_stats)

        return stats

    def collect(self):
        if beanstalkc is None:
            self.log.error('Unable to import beanstalkc')
            return {}

        instances = self.instances
        if isinstance(instances, basestring):
            instances = instances.split(',')

        for instance in instances:
            namespace,host,port = instance.split(':')
            info = self._get_stats(host,port)
            metric_prefix = "%s.beanstalkd.%s:%s." % (namespace,
                                                      host.replace('.', '_'),
                                                      port)

            for stat, value in info['instance'].items():
                if stat not in self.SKIP_LIST:
                    self.publish(metric_prefix + stat, value,
                             metric_type=self.get_metric_type(stat))

            for tube_stats in info['tubes']:
                tube = tube_stats['name']
                for stat, value in tube_stats.items():
                    if stat != 'name':
                        self.publish(metric_prefix + 'tubes.%s.%s' % (tube, stat), value,
                                 metric_type=self.get_metric_type(stat))

    def get_metric_type(self, stat):
        if self.COUNTERS_REGEX.match(stat):
            return 'COUNTER'
        return 'GAUGE'
