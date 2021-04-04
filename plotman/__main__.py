"""
Main executable script
"""
from typing import Union, List

from datetime import datetime
from io import TextIOWrapper
from subprocess import call

import argparse
import os
import re
import threading
import random
import readline          # For nice CLI
import sys
import time
import yaml

# Plotman libraries
from .job import Job
from .analyzer import LogAnalyzer
from . import archive
from . import interactive
from . import manager
from . import plot_util
from . import reporting


def arg_parser():
    def add_idprefix_arg(subparser):
        subparser.add_argument(
            'idprefix',
            type=str,
            nargs='+',
            help='disambiguating prefix of plot ID',
        )

    parser = argparse.ArgumentParser(description='Chia plotting manager.')
    sp = parser.add_subparsers(dest='cmd')

    p_status = sp.add_parser('status', help='show current plotting status')
    p_status.set_defaults(func=main_status)

    p_dirs = sp.add_parser('dirs', help='show directories info')
    p_dirs.set_defaults(func=main_dirs)

    p_interactive = sp.add_parser(
        'interactive', help='run interactive control/montioring mode'
    )
    p_interactive.set_defaults(func=main_interactive)

    p_dst_sch = sp.add_parser('dsched', help='print destination dir schedule')
    p_dst_sch.set_defaults(func=main_dsched)

    p_plot = sp.add_parser('plot', help='run plotting loop')
    p_plot.set_defaults(func=main_plot)

    p_archive = sp.add_parser(
        'archive', help='move completed plots to farming location'
    )
    p_archive.set_defaults(func=main_archive)

    p_details = sp.add_parser('details', help='show details for job')
    p_details.set_defaults(func=main_details)
    add_idprefix_arg(p_details)

    p_files = sp.add_parser(
        'files', help='show temp files associated with job'
    )
    p_files.set_defaults(func=main_files)
    add_idprefix_arg(p_files)

    p_kill = sp.add_parser('kill', help='kill job (and cleanup temp files)')
    p_kill.set_defaults(func=main_kill)
    add_idprefix_arg(p_kill)

    p_suspend = sp.add_parser('suspend', help='suspend job')
    p_suspend.set_defaults(func=main_suspend)
    add_idprefix_arg(p_suspend)

    p_resume = sp.add_parser('resume', help='resume suspended job')
    p_resume.set_defaults(func=main_resume)
    add_idprefix_arg(p_resume)

    p_analyze = sp.add_parser(
        'analyze', help='analyze timing stats of completed jobs'
    )
    p_analyze.set_defaults(func=main_analyze)
    p_analyze.add_argument(
        'logfile', type=str, nargs='+', help='logfile(s) to analyze'
    )

    return parser


class Configuration:
    def __init__(self, data):
        self.data = data

        self.polling_time = float(self.scheduling['polling_time_s'])

    @property
    def directories(self):
        return self.data['directories']

    @property
    def scheduling(self):
        return self.data['scheduling']

    @property
    def plotting(self):
        return self.data['plotting']

    @property
    def log(self):
        return self.data['directories']['log']

    @classmethod
    def from_file(cls, fp: Union[str, TextIOWrapper]):
        if isinstance(fp, str):
            with open(fp, 'r') as f:
                return cls.from_file(f)

        if fp.name.endswith('.yaml'):
            cfg = yaml.load(fp, Loader=yaml.FullLoader)
        else:
            raise NotImplementedError(
                f'Cannot load configuration from {fp.name!r}'
            )

        return cls(cfg)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    random.seed()

    parser = arg_parser()
    args = parser.parse_args(argv)

    cfg = Configuration.from_file('config.yaml')

    try:
        main_func = args.func
    except AttributeError:
        parser.print_help(file=sys.stderr)
        return 1

    return main_func(args, cfg)


def main_plot(args, cfg: Configuration):
    """Stay alive, spawning plot jobs"""
    print('...starting plot loop')
    while True:
        wait_reason = manager.maybe_start_new_plot(
            cfg.directories, cfg.scheduling, cfg.plotting
        )

        # TODO: report this via a channel that can be polled on demand, so we don't spam the console
        sleep_s = cfg.polling_time
        if wait_reason:
            print('...sleeping %g s: %s' % (sleep_s, wait_reason))

        time.sleep(sleep_s)


def main_status(args, cfg: Configuration):
    jobs = Job.get_running_jobs(cfg.log)
    (rows, columns) = os.popen('stty size', 'r').read().split()
    print(reporting.status_report(jobs, int(columns)))
    return 0


def main_analyze(args, cfg: Configuration):
    """Analysis of completed jobs"""
    analyzer = LogAnalyzer()
    analyzer.analyze(args.logfile)
    return 0


def main_dirs(args, cfg: Configuration):
    jobs = Job.get_running_jobs(cfg.log)
    (rows, columns) = os.popen('stty size', 'r').read().split()
    print(
        reporting.dirs_report(
            jobs, cfg.directories, cfg.scheduling, int(columns)
        )
    )
    return 0


def main_interactive(args, cfg: Configuration):
    interactive.run_interactive()
    return 0


def main_dsched(args, cfg: Configuration):
    """show the destination drive usage schedule"""
    dir_cfg = cfg.directories
    jobs = Job.get_running_jobs(dir_cfg.log)
    for (d, ph) in manager.dstdirs_to_furthest_phase(jobs).items():
        print('  %s : %s' % (d, str(ph)))
    return 0


def main_archive(args, cfg: Configuration):
    """Start running archival"""
    jobs = Job.get_running_jobs(cfg.log)
    print('...starting archive loop')
    firstit = True
    while True:
        if not firstit:
            print('Sleeping 60s until next iteration...')
            time.sleep(60)
            jobs = Job.get_running_jobs(cfg.log)
        firstit = False
        archive.archive(cfg.directories, jobs)
    return 0


def main_details(args, cfg: Configuration):
    selected = select_jobs(args, cfg.directories)
    for job in selected:
        print(job.status_str_long())
    return 0


def main_files(args, cfg: Configuration):
    selected = select_jobs(args, cfg.directories)
    for job in selected:
        temp_files = job.get_temp_files()
        for f in temp_files:
            print('  %s' % f)
    return 0


def main_kill(args, cfg: Configuration):
    selected = select_jobs(args, cfg.directories)
    for job in selected:
        # First suspend so job doesn't create new files
        print('Pausing PID %d, plot id %s' % (job.proc.pid, job.plot_id))
        job.suspend()

        temp_files = job.get_temp_files()
        print('Will kill pid %d, plot id %s' % (job.proc.pid, job.plot_id))
        print('Will delete %d temp files' % len(temp_files))
        conf = input('Are you sure? ("y" to confirm): ')
        if conf != 'y':
            print('canceled.  If you wish to resume the job, do so manually.')
        else:
            print('killing...')
            job.cancel()
            print('cleaing up temp files...')
            for f in temp_files:
                os.remove(f)
    return 0


def main_suspend(args, cfg: Configuration):
    selected = select_jobs(args, cfg.directories)
    for job in selected:
        print('Suspending ' + job.plot_id)
        job.suspend()
    return 0


def main_resume(args, cfg: Configuration):
    selected = select_jobs(args, cfg.directories)
    for job in selected:
        print('Resuming ' + job.plot_id)
        job.resume()
    return 0


def select_jobs(args, dir_cfg) -> List[Job]:
    """Select jobs based on partial id"""
    jobs = Job.get_running_jobs(dir_cfg['log'])

    if args.idprefix[0] == 'all':
        return jobs

    id_spec = args.idprefix[0]
    selected = manager.select_jobs_by_partial_id(jobs, id_spec)
    if len(selected) == 0:
        print('Error: %s matched no jobs.' % id_spec)
        raise SystemExit(1)
    elif len(selected) > 1:
        print('Error: "%s" matched multiple jobs:' % id_spec)
        for j in selected:
            print('  %s' % j.plot_id)
        selected = []

    return selected


if __name__ == '__main__':
    sys.exit(main())
