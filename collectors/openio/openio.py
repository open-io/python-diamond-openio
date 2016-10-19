# coding=utf-8

"""
Collect OpenIO SDS stats
Targeted SDS Version: B2 support (unstable)

#### Dependencies

 * urllib3
 * oio (previously oiopy, which has been merged into oio)
 * ast
 * json

"""

import diamond.collector
import diamond.convertor
import urllib3
import shlex
from oio.common import utils
import json
import os
from subprocess import Popen, PIPE


class OpenIOSDSCollector(diamond.collector.Collector):

    def process_config(self):
        super(OpenIOSDSCollector, self).process_config()
        self.namespaces = self.config['namespaces'] or 'OPENIO'
        self.fs = self.config['fs-types'] or ('xfs', 'ext4')

    def get_default_config_help(self):
        config_help = super(OpenIOSDSCollector, self).get_default_config_help()
        config_help.update({
            'namespaces': "List of namespaces (comma separated)",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(OpenIOSDSCollector, self).get_default_config()
        config.update({
            'path': 'openio',
            'byte_unit': ['byte']
        })
        return config

    def collect(self):
        namespaces = self.namespaces

        # Convert a string config value to be an array
        if isinstance(namespaces, basestring):
            namespaces = [namespaces]

        http = urllib3.PoolManager()
        for ns in namespaces:
            config = utils.load_namespace_conf(ns)
            if not config:
                self.log.error('No configuration found for namespace ' + ns)
                continue
            proxy = config['proxy']
            self.get_stats(http, ns, proxy)

    def get_stats(self, http, namespace, proxy):
        try:
            srvtypes = json.loads(http.request(
                'GET',
                "%s/v3.0/%s/conscience/info?what=types" % (proxy, namespace)
                ).data)
        except Exception as exc:
            self.log.error("Unable to connect to proxy at %s: %s", proxy, exc)
            return
        for srvtype in srvtypes:
            try:
                services = http.request('GET',
                                        "%s/v3.0/%s/conscience/list?type=%s" %
                                        (proxy, namespace, srvtype))
            except Exception as exc:
                self.log.error("Unable to connect to proxy at %s: %s",
                               proxy, exc)
                return
            services = json.loads(services.data)
            # assume that all local services are listening
            # on the same IP address as the proxy
            proxy_ip = proxy.split('//', 1)[-1].split(":", 1)[0] + ":"
            for s in (x for x in services
                      if x.get('addr', "").startswith(proxy_ip)):
                metric_prefix = "%s.%s.%s" % (namespace,
                                              srvtype,
                                              s['addr'].replace('.', '_'))
                metric_value = self.cast_str(s['score'])
                if not isinstance(metric_value, basestring):
                    self.publish(metric_prefix + ".score", metric_value)
                if srvtype == 'rawx':
                    self.get_service_diskspace(
                        metric_prefix, s.get("tags", {}).get('tag.vol', '/'))
                    self.get_rawx_stats(http, s['addr'], namespace)
                elif srvtype == 'meta2':
                    self.get_gridd_stats(http, proxy, s['addr'],
                                         namespace, srvtype)

    def get_filesystem(self, volume):
        """ Fetches the the block id of a provided volume to include
        as an additional tag
        """
        with open("/etc/mtab") as f:
            rootfs = None
            for line in f:
                l = line.split(' ')
                if l[1] == "/":
                    rootfs = line
                elif any([True for e in self.fs if e in line]):
                    if volume.startswith(l[1]):
                        return self.get_blkid(line, volume)
            if rootfs:
                return self.get_blkid(rootfs, volume)

    def get_blkid(self, line, volume):
        try:
            p = Popen(['blkid', line[0]], stdout=PIPE,
                      stderr=PIPE)
            stdout, stderr = p.communicate()
            params = dict(t.split('=')
                          for t in shlex.split("VOLUME="+stdout))
            return str(params['UUID'])
        except Exception as e:
            self.log.exception(e)
            return line[0]

    def get_service_diskspace(self, metric_prefix, volume):
        if hasattr(os, 'statvfs'):  # POSIX
            try:
                data = os.statvfs(volume)
            except OSError as e:
                self.log.exception(e)
                return
            block_size = data.f_bsize
            blocks_total = data.f_blocks
            blocks_free = data.f_bfree
            blocks_avail = data.f_bavail
            inodes_total = data.f_files
            inodes_free = data.f_ffree
            inodes_avail = data.f_favail
        else:
            raise NotImplementedError("platform not supported")

        dvolume = self.get_filesystem(volume).replace("/", "-")[1:]
        dvolume = dvolume.replace('_', '-')

        for unit in self.config['byte_unit']:
            metric_name = '%s.%s.%s_percentfree'\
                            % (metric_prefix, dvolume, unit)
            metric_value = float(blocks_free) / float(
                blocks_free + (blocks_total - blocks_free)) * 100
            self.publish_gauge(metric_name, metric_value, 2)

            metric_name = '%s.%s.%s_used' % (metric_prefix, dvolume, unit)
            metric_value = float(block_size) * float(
                blocks_total - blocks_free)
            metric_value = diamond.convertor.binary.convert(
                value=metric_value, oldUnit='byte', newUnit=unit)
            self.publish_gauge(metric_name, metric_value, 2)

            metric_name = '%s.%s.%s_free' % (metric_prefix, dvolume, unit)
            metric_value = float(block_size) * float(blocks_free)
            metric_value = diamond.convertor.binary.convert(
                value=metric_value, oldUnit='byte', newUnit=unit)
            self.publish_gauge(metric_name, metric_value, 2)

            metric_name = '%s.%s.%s_avail' % (metric_prefix, dvolume, unit)
            metric_value = float(block_size) * float(blocks_avail)
            metric_value = diamond.convertor.binary.convert(
                value=metric_value, oldUnit='byte', newUnit=unit)
            self.publish_gauge(metric_name, metric_value, 2)

        if float(inodes_total) > 0:
            self.publish_gauge(
                '%s.inodes_percentfree' % metric_prefix,
                float(inodes_free) / float(inodes_total) * 100)
        self.publish_gauge('%s.inodes_used' % metric_prefix,
                           inodes_total - inodes_free)
        self.publish_gauge('%s.inodes_free' % metric_prefix, inodes_free)
        self.publish_gauge('%s.inodes_avail' % metric_prefix, inodes_avail)

    def get_rawx_stats(self, http, addr, namespace, srv_type='rawx'):
        stat = http.request('GET', addr+'/stat')
        for m in (stat.data).split('\n'):
            if not m:
                continue
            metric_type, metric_name, metric_value = m.split(' ')
            metric_value = self.cast_str(metric_value)
            if not isinstance(metric_value, basestring):
                metric_name = "%s.%s.%s.%s" % (namespace,
                                               srv_type,
                                               addr.replace('.', '_'),
                                               metric_name)
                self.publish(metric_name, metric_value,
                             metric_type=metric_type.upper())

    def get_gridd_stats(self, http, proxy, addr, namespace, srv_type):
        stat = http.request('POST', proxy+'/v3.0/forward/stats?id='+addr)
        for m in (stat.data).split('\n'):
            if not m:
                continue
            metric_type, metric_name, metric_value = m.split(' ')
            metric_value = self.cast_str(metric_value)
            if not isinstance(metric_value, basestring):
                metric_name = "%s.%s.%s.%s" % (namespace,
                                               srv_type,
                                               addr.replace('.', '_'),
                                               metric_name)
                self.publish(metric_name, metric_value,
                             metric_type=metric_type.upper())

    def cast_str(self, value):
        """Return string casted to int or float if possible"""
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
