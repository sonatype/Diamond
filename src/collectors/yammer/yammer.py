# coding=utf-8

"""
Collect [Yammer Metrics](http://metrics.codahale.com/) metrics via HTTP 

#### Dependencies

 * urlib2

"""

import urllib2

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector


class YammerCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(YammerCollector,
                            self).get_default_config_help()
        config_help.update({
            'url': 'URL from which to pull metrics',
            'username': 'Username if basic auth is required',
            'password': 'Password is basic auth is required',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(YammerCollector, self).get_default_config()
        config.update({
            'path':     'yammer',
            'url':      'http://127.0.0.1:8081/metrics',
            'username': '',
            'password': '',
        })
        return config

    def collect(self):
        if json is None:
            self.log.error('Unable to import json')
            return {}
        try:
            if username:
                passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
                passman.add_password(None, self.config['url'], self.config['username'], self.config['password'])
                urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman)))
            response = urllib2.urlopen(self.config['url'])
        except urllib2.HTTPError, err:
            self.log.error("%s: %s", url, err)
            return

        try:
            result = json.load(response)
        except (TypeError, ValueError):
            self.log.error("Unable to parse response from elasticsearch as a"
                           + " json object")
            return

        metrics = {}

        for k, v in result.items():
            k = self._sanitize(k)
            metrics.update(self._parseMetrics(k,v))

        for key in metrics:
            self.publish(key, metrics[key])

    def _sanitize(self, name):
        return name.replace(' ','_').replace('.','_').replace('-','_')
       
    def _flatten(self,di):
        stack = [('',di)]
        while stack:
            e = stack[-1]
            for k, v in e[1].items():
                if e[0]:
                    name = e[0] + '.' + self._sanitize(k)
                else:
                    name = self._sanitize(k)
                if isinstance(v, dict):
                    stack.append((name,v))
                else:
                    yield name, v
            stack.remove(e)

    def _parseMetrics(self,prefix,raw_metrics):
        metrics = {}

        for k, v in self._flatten(raw_metrics):
            if isinstance(v,int) or isinstance(v,float):
                metrics[prefix + '.' + k] = v

        return metrics
