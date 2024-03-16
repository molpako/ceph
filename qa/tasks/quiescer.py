"""
Thrash mds by simulating failures
"""
import logging
import contextlib

from gevent import sleep
from teuthology import misc as teuthology

from tasks import ceph_manager
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.thrasher import ThrasherGreenlet

import random
import math
import errno
import json
import time
import sys

from io import StringIO

log = logging.getLogger(__name__)

class Quiescer(ThrasherGreenlet):
    """
    The Quiescer does periodic quiescing of the configured paths, by default - the root '/'.

    quiesce_timeout: maximum time in seconds to wait for the quiesce to succeed
    quiesce_factor: the percentage of time we allow the system to stay quiesced
    min_quiesce: the minimum pause time in seconds
    max_quiesce: the maximum pause time in seconds
    initial_delay: the time in seconds before the first quiesce
    seed: optional seed to a pseudorandom sequence of quiesce durations
    """

    MAX_QUIESCE_FACTOR = 0.5    # 50%
    MIN_QUIESCE_FACTOR = 0.005  # 0.5%

    def __init__(self, fs, quiesce_timeout=30, quiesce_factor=0.1, min_quiesce=10, max_quiesce=60, initial_delay=120, seed=None, **unused_kwargs):
        super(Quiescer, self).__init__()

        self.logger = log.getChild('fs.[{f}]'.format(f=fs.name))
        self.fs = fs
        self.name = 'quiescer.fs.[{f}]'.format(f=fs.name)
        self.rnd = random.Random()
        if seed is None:
            seed = self.rnd.randint(0, 999999) # 1M seems sufficient and avoids possible huge numbers

        self.logger.info(f"Initializing Quiescer with random seed {seed}")
        self.rnd.seed(seed)

        self.quiesce_timeout = quiesce_timeout

        if (quiesce_factor > self.MAX_QUIESCE_FACTOR):
            self.logger.warn("Capping the quiesce factor at %f (requested: %f)" % (self.MAX_QUIESCE_FACTOR, quiesce_factor))
            quiesce_factor = self.MAX_QUIESCE_FACTOR

        if quiesce_factor < self.MIN_QUIESCE_FACTOR:
            self.logger.warn("setting the quiesce factor to %f (requested: %f)" % (self.MIN_QUIESCE_FACTOR, quiesce_factor))
            quiesce_factor = self.MIN_QUIESCE_FACTOR


        self.quiesce_factor = max(0.01, quiesce_factor)
        self.min_quiesce = min_quiesce
        self.max_quiesce = max_quiesce
        self.initial_delay = initial_delay

    def next_quiesce_duration(self):
        mu = (self.min_quiesce + self.max_quiesce) / 2
        sigma = 3 * math.sqrt(self.max_quiesce - self.min_quiesce)
        duration = round(self.rnd.gauss(mu, sigma), 1)
        duration = max(duration, self.min_quiesce)
        duration = min(duration, self.max_quiesce)
        return duration

    def tell_quiesce_leader(self, *args):
        leader = None
        rc = None
        stdout = None

        while leader is None and not self.is_stopped:
            leader = self.fs.get_var('qdb_leader')
            if leader is None:
                self.logger.warn("Couldn't get quiesce db leader from the mds map")
                self.sleep_unless_stopped(5)

        while leader is not None and not self.is_stopped:
            # We use the one_shot here to cover for cases when the mds crashes
            # without this parameter the client may get stuck awaiting response from a dead MDS
            command = ['tell', f"mds.{leader}", 'quiesce', 'db']
            command.extend(args)
            self.logger.debug("Running ceph command: '%s'" % " ".join(command))
            result = self.fs.run_ceph_cmd(args=command, check_status=False, stdout=StringIO())
            rc, stdout = result.exitstatus, result.stdout.getvalue()
            if rc == -errno.ENOTTY:
                try:
                    resp = json.loads(stdout)
                    leader = int(resp['leader'])
                    self.logger.info("Retrying a quiesce db command with leader %d" % leader)
                except Exception as e:
                    self.logger.error("Couldn't parse ENOTTY response from an mds with error: %s\n%s" % (str(e), stdout))
                    self.sleep_unless_stopped(5)
            else:
                break

        return (rc, stdout)
    
    def get_set_state_name(self, response, set_id = None):
        if isinstance(response, (str, bytes, bytearray)):
            response = json.loads(response)

        sets = response['sets']
        if len(sets) == 0:
            raise ValueError("response has no sets")

        if set_id is None:
            if len(sets) > 1:
                raise ValueError("set_id must be provided for a multiset response")
            else:
                set_id = next(iter(sets.keys()))

        return response['sets'][set_id]['state']['name']

    def check_canceled(self, response, set_id = None):
        if 'CANCELED' == self.get_set_state_name(response, set_id):
            self.logger.warn('''
                             Quiesce set got cancelled. Won't raise an error since this could be a failover, 
                             will wait for the next quiesce attempt''')
            return True
        return False
            
    
    def do_quiesce(self, duration):
        
        start_time = time.time()
        self.logger.info(f"Going to quiesce for duration: {duration}")

        # quiesce the root
        rc, stdout = self.tell_quiesce_leader(
            "/", # quiesce at the root
            "--timeout", str(self.quiesce_timeout), 
            "--expiration", str(duration + 60), # give us a minute to run the release command
            "--await" # block until quiesced (or timedout)
        )

        try:
            response = json.loads(stdout)
            set_id = next(iter(response["sets"].keys()))
        except Exception as e:
            self.logger.error(f"Couldn't parse response with error {e}; stdout:\n{stdout}")
            raise RuntimeError(f"Error parsing quiesce response: {e}")


        if self.check_canceled(response):
            return

        if rc != 0:
            rcinfo = f"{-rc} ({errno.errorcode.get(-rc, 'Unknown')})"
            self.logger.error(f"Couldn't quiesce root with rc: {rcinfo}, stdout:\n{stdout}")
            raise RuntimeError(f"Error quiescing root: {rcinfo}")

        self.logger.info(f"Successfully quiesced, set_id: {set_id}, quiesce duration: {duration}")
        self.sleep_unless_stopped(duration)

        # release the root
        rc, stdout = self.tell_quiesce_leader(
            "--set-id", set_id,
            "--release",
            "--await"
        )
        
        if rc != 0:
            if self.check_canceled(stdout, set_id):
                return

            rcinfo = f"{-rc} ({errno.errorcode.get(-rc, 'Unknown')})"
            self.logger.error(f"Couldn't release root with rc: {rcinfo}, stdout:\n{stdout}")
            raise RuntimeError(f"Error releasing root: {rcinfo}")
        else:
            elapsed = round(time.time() - start_time, 1)
            self.logger.info(f"Successfully released set_id: {set_id}, seconds elapsed: {elapsed}")


    def _run(self):
        try:
            self.sleep_unless_stopped(self.initial_delay)

            while not self.is_stopped:
                duration = self.next_quiesce_duration()
                self.do_quiesce(duration)
                # now we sleep to maintain the quiesce factor
                self.sleep_unless_stopped((duration/self.quiesce_factor) - duration)

        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def stop(self):
        self.tell_quiesce_leader( "--cancel", "--all" )
        super(Quiescer, self).stop()


def stop_all_quiescers(thrashers):
    for thrasher in thrashers:
        if not isinstance(thrasher, Quiescer):
            continue
        thrasher.stop()
        thrasher.join()
        if thrasher.exception is not None:
            raise RuntimeError(f"error during scrub thrashing: {thrasher.exception}")


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the mds by randomly quiescing the whole FS while another task/workunit
    is running.
    Example config (see class initializer for all available options):

    - quiescer:
        quiesce_factor: 0.2
        max_quiesce: 30
        quiesce_timeout: 10
    """

    mds_cluster = MDSCluster(ctx)

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'quiescer task only accepts a dict for configuration'
    mdslist = list(teuthology.all_roles_of_type(ctx.cluster, 'mds'))
    assert len(mdslist) > 0, \
        'quiescer task requires at least 1 metadata server'

    (first,) = ctx.cluster.only(f'mds.{mdslist[0]}').remotes.keys()
    manager = ceph_manager.CephManager(
        first, ctx=ctx, logger=log.getChild('ceph_manager'),
    )

    # make sure everyone is in active, standby, or standby-replay
    log.info('Wait for all MDSs to reach steady state...')
    status = mds_cluster.status()
    while True:
        steady = True
        for info in status.get_all():
            state = info['state']
            if state not in ('up:active', 'up:standby', 'up:standby-replay'):
                steady = False
                break
        if steady:
            break
        sleep(2)
        status = mds_cluster.status()

    log.info('Ready to start quiesce thrashing')

    manager.wait_for_clean()
    assert manager.is_clean()

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    for fs in status.get_filesystems():
        quiescer = Quiescer(Filesystem(ctx, fscid=fs['id']), **config)
        quiescer.start()
        ctx.ceph[config['cluster']].thrashers.append(quiescer)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining Quiescers')
        stop_all_quiescers(ctx.ceph[config['cluster']].thrashers)
        log.info('done joining Quiescers')
