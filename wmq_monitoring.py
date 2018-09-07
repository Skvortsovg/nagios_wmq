#!/usr/bin/python
# -*- coding: utf-8 -*-

from contextlib import closing

import sys
sys.path.append('/var/tmp/')

import pymqi as mq
import argparse
import ConfigParser

class NagiosExitCodes:
    OK = 0
    WARNING = 1
    CRITICAL = 2
    UNKNOWN = 3

class cached_property(object):

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


def conf_manager():
    prog = 'WMQ monitoring'
    __version__ = '0.0.1'
    description = 'Script for WMQ monitoring'
    epilog = '' 

    parser = argparse.ArgumentParser(prog=prog,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=description,
                                     epilog=epilog)

    parser.add_argument('-a', '--add',
                        default=False,
                        action='store_true',
                        help='Add new queue')

    parser.add_argument('-rm', '--remove',
                        default=False,
                        action='store_true',
                        help='Remove queue')

    parser.add_argument('-e', '--edit',
                        default=False,
                        action='store_true',
                        help='Edit queue')

    parser.add_argument('-l', '--list',
                        default=False,
                        action='store_true',
                        help='Show config')

    parser.add_argument('-c', '--config',
                        default='/usr/lib64/nagios/plugins/wmq_queues_list.conf',
                        action='store',
                        help='Config filename')

    parser.add_argument('-qm', '--qmgr',
                        action='store',
                        help='Queue manager')

    parser.add_argument('-H', '--host',
                        action='store',
                        help='IP or hostname')

    parser.add_argument('-p', '--port',
                        action='store',
                        help='Port',
                        default=1414)

    parser.add_argument('-ch', '--channel',
                        action='store',
                        help='Channel name')

    opts, args = parser.parse_known_args()
    return opts


def get_queues_info():

    config = ConfigParser.ConfigParser()
    config.read(conf_manager().config)
    config_dict = {}
    for queue_name in config.sections():
        try:
            warning_depth = int(config.get(queue_name, 'warning_depth'))
            critical_depth = int(config.get(queue_name, 'critical_depth'))
        except ValueError, ex:
            pass
        config_dict[queue_name] = {'warning_depth': warning_depth,
                                   'critical_depth': critical_depth}
    return config_dict

class WMQ:

    def __init__(self, host, port, queue_manager, channel, user=None, password=None):
        self.queue_manager = queue_manager
        self.channel = channel
        self.conn_info = "%s(%s)" % (host, port)
        self.qmgr = None
        self.user = user
        self.password = password

    def connect(self):
        opts = []
        if self.user and self.password:
            opts.extend([self.user, self.password])
        self.qmgr = mq.connect(self.queue_manager, self.channel, self.conn_info, *opts)

    def disconnect(self):
        if self.qmgr:
            self.qmgr.disconnect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def iter_channels(self, channel_prefix=None):
        if channel_prefix is None:
            channel_prefix = '*'
        args = {mq.CMQCFC.MQCACH_CHANNEL_NAME: channel_prefix}
        try:
            rv = self.pcf.MQCMD_INQUIRE_CHANNEL(args)
        except mq.MQMIError, err:
            if err.comp == mq.CMQC.MQCC_FAILED and \
                    err.reason == mq.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
                raise StopIteration()
            else:
                raise
        for channel_info in rv:
            yield channel_info

    def iter_queues(self, queue_prefix=None, queue_type=None):
        if queue_prefix is None:
            queue_prefix = '*'
        args = {
            mq.CMQC.MQCA_Q_NAME: queue_prefix,
        }
        if queue_type is not None:
            args[mq.CMQC.MQQT_MODEL] = queue_type
        try:
            rv = self.pcf.MQCMD_INQUIRE_Q(args)
        except mq.MQMIError, err:
            if err.comp == mq.CMQC.MQCC_FAILED and \
                    err.reason == mq.CMQC.MQRC_UNKNOWN_OBJECT_NAME:
                raise StopIteration()
            else:
                raise
        for queue_info in rv:
            yield queue_info

    def iter_messages(self, queue_name):
        opts = mq.gmo(Options=mq.CMQC.MQGMO_BROWSE_NEXT)
        opts.WaitInterval = mq.CMQC.MQWI_UNLIMITED
        with closing(self.queue(queue_name, mq.CMQC.MQOO_BROWSE)) as queue:
            while True:
                try:
                    msg_desc = mq.md()
                    msg = queue.get(None, msg_desc, opts)
                    yield msg_desc, msg
                except mq.MQMIError:
                    break

    @cached_property
    def pcf(self):
        return mq.PCFExecute(self.qmgr)

    def ping(self):
        # pcf = mq.PCFExecute(self.qmgr)
        return self.pcf.MQCMD_PING_Q_MGR()

    def queue(self, queue_name, opts=None):
        if opts is not None:
            return mq.Queue(self.qmgr, queue_name, opts)
        else:
            return mq.Queue(self.qmgr, queue_name)

    @staticmethod
    def queue_depth(self, queue_name):
        queue = self.queue(queue_name)
        return queue.inquire(mq.CMQC.MQIA_CURRENT_Q_DEPTH)
    
    @staticmethod
    def queue_oldest_msg(self, queue_name):
        queue = self.ueue(queue_name)
        return queue.inquire(mq.CMQCFC.MQIACF_OLDEST_MSG_AGE)



def main():
    opts = conf_manager()
    queues_dict = get_queues_info()
    exit_code = NagiosExitCodes.OK
    status_info = []
    with WMQ(opts.host, opts.port, opts.qmgr, opts.channel) as q:
        for q_name in queues_dict:
            try:
                q_depth = q.queue_depth(q_name)
            except:
                q_depth = -1
            
            if q_depth >= queues_dict[q_name]['critical_depth']:
                status_info.append('{q_name} depth is {q_depth}'.format(q_name=q_name, q_depth=q_depth))
                exit_code = NagiosExitCodes.CRITICAL
                continue
            elif queues_dict[q_name]['warning_depth'] <= q_depth < queues_dict[q_name]['critical_depth']:
                status_info.append('{q_name} depth is {q_depth}'.format(q_name=q_name, q_depth=q_depth))
                exit_code = max(NagiosExitCodes.WARNING, exit_code)
                continue
            elif q == -1:
                exit_code = NagiosExitCodes.CRITICAL
                status_info.append('{q_name} not found'.format(q_name=q_name))
    print 'OK' if not status_info else '\n'.join(status_info)
    raise SystemExit(exit_code)            



if __name__ == '__main__':
    main()