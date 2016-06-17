# coding=utf-8

"""
Collect metrics from Backblaze API
Targeted SDS Version: B2 support (unstable)

#### Dependencies

 * oio

"""

import diamond.collector
from oio.api.backblaze_http import Backblaze, BackblazeException


class BackblazeCollector(diamond.collector.Collector):

    def process_config(self):
        super(BackblazeCollector, self).process_config()
        self.application_key = self.config['application-key']
        self.account_id = self.config['account-id']
        self.bucket_name = self.config['bucket-name']

    def get_default_config_help(self):
        config_help = super(BackblazeCollector, self).get_default_config_help()
        config_help.update({
            "account-id": "",
            "application-key": "",
            "bucket-name": ""
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(BackblazeCollector, self).get_default_config()
        config.update({
            'path': 'backblaze'
        })
        return config

    def collect(self):
        """
        Overrides the Collector.collect method
        """
        backblaze = Backblaze(self.account_id, self.application_key)
        # Set Metric Name
        metric_backblaze_size = "%s.%s.space" % (self.account_id,
                                                 self.bucket_name)
        metric_backblaze_number = '%s.%s.number' % (self.account_id,
                                                    self.bucket_name)
        # Set Metric Value
        try:
            size, number = backblaze.get_backblaze_infos(self.bucket_name)
            self.publish(metric_backblaze_size, size)
            self.publish(metric_backblaze_number, number)
        except BackblazeException as e:
            self.log.error(e)
