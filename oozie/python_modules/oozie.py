#!/usr/bin/env python
# vim: ts=4:sw=4:et:sts=4:ai:tw=80

try:
    import simplejson as json
    assert json
except ImportError:
    import json

from functools import partial

import sys
import os
import logging

from ooziestatus import OozieStatus

logging.basicConfig(filename='/var/log/ganglia.oozie.log',level=logging.DEBUG)

def create_desc(skel, prop):
    d = skel.copy()
    for k, v in prop.iteritems():
        d[k] = v
    return d

def metric_init(params):
    descriptors = []

    metric_group = params.get('metric_group','oozie')
    Desc_Skel = {
        'name': 'XXX',
        'call_back':'XXX', # partial(getStat, result, url_cluster),
        'time_max': 60,
        'value_type': 'uint',
        'units': 'units',
        'slope': 'both',
        'format': '%d',
        'description': 'XXX',
        'groups': metric_group,
    }
    _create_desc = partial(create_desc, Desc_Skel)
    oozie = OozieStatus(params)
    
    for coord,status in oozie.coordinators.iteritems():
        for stat,val in status.iteritems():
            if stat == 'total':
                continue
            descriptors.append(
                _create_desc({
                    'name':'oozie_'+coord+'-'+stat+'-percent',
                    'units':'%',
                    'call_back':partial(dummy_feeder,float(val['count'])*100/float(status['total'])),
                    'format':'%f',
                    'value_type':'float',
                    'description':'Shows % of actions finishing in each state'}))
            descriptors.append(
                _create_desc({
                    'name':'oozie_'+coord+'-'+stat+'-runtime',
                    'units':'sec',
                    'call_back':partial(dummy_feeder,float(val['runtime']/val['count'])),
                    'format':'%f',
                    'value_type':'float',
                    'description':'Shows avearage time taken by actions finishing in a determinate state'}))
    return descriptors

def dummy_feeder(value,name):
    return value
    
#This code is for debugging and unit testing
if __name__ == '__main__':
    descriptors = metric_init({'host':'host','port':11000,'secure':True, 'keytab':'/path/to/keytab','principal':'user/instance'})
    for d in descriptors:
        v = d['call_back'](d['name'])
        logging.debug('value for %s is %s' % (d['name'], str(v)))
        print 'value for %s is %s' % (d['name'], str(v))
