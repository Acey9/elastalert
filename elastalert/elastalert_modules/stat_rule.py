#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import dateutil.parser
from ruletypes import RuleType
from util import pretty_ts
from util import ts_to_dt
from util import dt_to_ts

class StatRule(RuleType):
    '''support
       stat function in (sum, )
       stat_type in (greater, less, equal)
    '''
    required_options = set(['stat', 'threshold', 'stat_type'])

    def __init__(self, *args):
        super(StatRule, self).__init__(*args)
        self.stat_function = {
                'sum':self._sum,
                }

        self.op = {
                'greater':self.greater,
                'less':self.less,
                'equal':self.equal,
                }

        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        self.stat_field = self.rules['stat_field']
        self.group_by_field = self.rules.get("group_by")
        self.threshold =  self.rules['threshold']
        self.stat = self.rules['stat']
        self.stat_type = self.rules['stat_type']
        self.match_value = []

    def _sum(self, data):
        return sum([d[self.stat_field] for d in data])

    def greater(self, p1, p2):
        return p1 > p2

    def less(self, p1, p2):
        return p1 < p2

    def equal(self, p1, p2):
        return p1 == p2

    def add_data(self, data):
        self.check_for_match(data)

    def group_by(self, data):
        group = {}
        for event in data:
            if event.get(self.stat_field) is None or event.get(self.group_by_field) is None:
                continue
            field_value = event[self.group_by_field]
            group.setdefault(field_value, [])
            group[field_value].append(event)
        return group

    def check_for_match(self,data):

        stat_func = self.stat_function.get(self.stat)
        stat_value_dict = {}
        if not self.group_by_field:
            stat_value_dict['all'] = stat_func(data)
        else:
            group_data = self.group_by(data)
            for field_value, _data in group_data.iteritems():
                stat_value_dict[field_value] = stat_func(_data)

        match_success = False
        match_value = []
        for field_value, stat_value in stat_value_dict.iteritems():
            match_success = self.op.get(self.stat_type)(stat_value, self.threshold)
            if match_success:
                match_value.append(field_value)
        if match_value:
            self.match_value.append(match_value)

        if match_success:
            self.add_match(data[0])

    def get_match_str(self, match):
        ts = match[self.rules['timestamp_field']]
        lt = self.rules.get('use_local_time')

        try:
            match_value = self.match_value[-1][:5]
        except:
            match_value = []

        message =  "Between %s and %s\n" % (pretty_ts(dt_to_ts(ts_to_dt(ts) - self.rules['timeframe']), lt), pretty_ts(ts, lt))
        message += "%s(%s) %s %s\nmatch value:\n\t%s...\n\n" % (
                self.rules['stat'],
                self.rules['stat_field'],
                self.rules['stat_type'],
                self.rules['threshold'],
                '\n\t'.join(match_value)
                ) 
        return message

    def garbage_collect(self, timestamp):
        if len(self.match_value) > 1:
            self.match_value = self.match_value[:-1]
