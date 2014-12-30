from __future__ import absolute_import

import os
import re

from glob import glob
from ceres import CeresTree, CeresNode
from ..node import BranchNode, LeafNode
from ..intervals import Interval, IntervalSet
from ..utils import RequestParams
# from ..carbonlink import CarbonLinkPool
# import traceback
import structlog
logger = structlog.get_logger()

from . import get_real_metric_path
import time


class CeresReader(object):
    """
    Read data from ceres
    """
    __slots__ = ('ceres_node', 'real_metric_path', 'carbonlink')
    supported = True

    def __init__(self, ceres_node, real_metric_path):
        self.ceres_node = ceres_node
        self.real_metric_path = real_metric_path
#        self.carbonlink = CarbonLinkPool(app.app.config['FULL'])

    def get_intervals(self):
        """
        Get list of intervals
        """
        intervals = list()
        for info in self.ceres_node.slice_info:
            (start, end, step) = info
            intervals.append(Interval(start, end))

        return IntervalSet(sorted(intervals))

    def fetch(self, start_time, end_time):
        """
        Fetch data from ceres
        :param start_time:
        :param end_time:
        :returns: tuple with time_info about fetched data and with values for given interval
        """
        data = self.ceres_node.read(start_time, end_time)
        time_info = (data.startTime, data.endTime, data.timeStep)
        values = list(data.values)

        # Merge in data from carbon's cache
        # CarbonLink support disabled for now. We are not caching data that much
#        try:
#            cached_datapoints = self.carbonlink.query(self.real_metric_path)
#        except Exception as e:
#            trace = traceback.format_exc()
#            logger.error("Failed CarbonLink query '%s', reason: %s\ntrace:\n%s" %
#                         (self.real_metric_path, str(e), str(trace)))
#            cached_datapoints = list()
        cached_datapoints = list()

        for (timestamp, value) in cached_datapoints:
            interval = timestamp - (timestamp % data.timeStep)

            try:
                i = int(interval - data.startTime) / data.timeStep
                values[i] = value
            except IndexError:
                pass

        return time_info, values


class CeresNullReader(object):
    """
    Ceres-compatible readers that returns Nulls.
    Nulls are generated based on Ceres's metadata
    """
    __slots__ = ('ceres_node', 'real_metric_path', 'carbonlink')
    supported = True

    def __init__(self, ceres_node, real_metric_path):
        self.ceres_node = ceres_node
        self.real_metric_path = real_metric_path

    def get_intervals(self):
        intervals = []
        for info in self.ceres_node.slice_info:
            (start, end, step) = info
            intervals.append(Interval(start, end))

        return IntervalSet(sorted(intervals))

    def fetch(self, start_time, end_time):
        metadata = self.ceres_node.readMetadata()
        from_time = int(start_time - (start_time % self.ceres_node.timeStep))
        until_time = int(end_time - (end_time % self.ceres_node.timeStep))
        now = int(time.time())
        biggest_tiemstep = 60
        try:
            biggest_timestep = metadata["timeStep"]
            tmp = 0
            for ts in metadata["retentions"]:
                tmp += ts[0] * ts[1]
                if from_time > now - tmp:
                    break
                biggest_timestep = ts[0]
        except TypeError:
            pass
        missing = int(until_time - from_time) / biggest_timestep
        result_values = [None for i in range(missing)]

        return (from_time, until_time, biggest_timestep), list(result_values)


def normalize_config(config=None):
    """
    Compatibility layer for both graphite-web and graphite-api
    :param config:
    :return:
    """
    ret = {}
    if config is not None:
        cfg = config.get('ceres', {})
        ret['dir'] = cfg.get('ceres_dir', '/srv/storage/ceres')
    else:
        from django.conf import settings
        ret['dir'] = getattr(settings, 'CERES_DIR', '/srv/storage/ceres')
    return ret


class CeresFinder(object):
    __split_re = re.compile(r'{([^}]+)}(.*)')
    re_braces = re.compile(r'({[^{},]*,?[^{}]*})')

    def __init__(self, config=None):
        config = normalize_config(config)
        self.directory = config['dir']
        self.tree = CeresTree(self.directory)

    def braces_glob(self, s):
        """
        Graphite-style globbing
        :param s:
        :return:
        """
        match = self.re_braces.search(s)

        if not match:
            return glob(s)

        res = set()
        sub = match.group(1)
        open_pos, close_pos = match.span(1)

        for bit in sub.strip('{}').split(','):
            res.update(self.braces_glob(s[:open_pos] + bit + s[close_pos:]))
        return list(res)

    def find_nodes(self, query):
        """
        Find Ceres nodes that matches query
        :param query:
        :return:
        """
        fs_paths = self.braces_glob(self.tree.getFilesystemPath(query.pattern))
        for fs_path in self.braces_glob(self.tree.getFilesystemPath(query.pattern)):
            metric_path = self.tree.getNodePath(fs_path)

            if CeresNode.isNodeDir(fs_path):
                ceres_node = self.tree.getNode(metric_path)

                real_metric_path = get_real_metric_path(fs_path, metric_path)
                if ceres_node.hasDataForInterval(query.startTime, query.endTime):
                    reader = CeresReader(ceres_node, real_metric_path)
                else:
                    if "withNulls" in RequestParams:
                        reader = CeresNullReader(ceres_node, real_metric_path)
                    else:
                        continue
                yield LeafNode(metric_path, reader)
            elif os.path.isdir(fs_path):
                yield BranchNode(metric_path)

