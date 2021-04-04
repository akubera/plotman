"""
Main executable script
"""

from datetime import datetime
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
                help='disambiguating prefix of plot ID')

    parser = argparse.ArgumentParser(description='Chia plotting manager.')
    sp = parser.add_subparsers(dest='cmd')

    p_status = sp.add_parser('status', help='show current plotting status')
    p_status.set_defaults(func=main_status)

    p_dirs = sp.add_parser('dirs', help='show directories info')

    p_interactive = sp.add_parser('interactive', help='run interactive control/montioring mode')

    p_dst_sch = sp.add_parser('dsched', help='print destination dir schedule')

    p_plot = sp.add_parser('plot', help='run plotting loop')
    p_plot.set_defaults(func=main_plot)

    p_archive = sp.add_parser('archive',
            help='move completed plots to farming location')

    p_details = sp.add_parser('details', help='show details for job')
    add_idprefix_arg(p_details)

    p_files = sp.add_parser('files', help='show temp files associated with job')
    add_idprefix_arg(p_files)

    p_kill = sp.add_parser('kill', help='kill job (and cleanup temp files)')
    add_idprefix_arg(p_kill)

    p_suspend = sp.add_parser('suspend', help='suspend job')
    add_idprefix_arg(p_suspend)

    p_resume = sp.add_parser('resume', help='resume suspended job')
    add_idprefix_arg(p_resume)

    p_analyze = sp.add_parser('analyze', help='analyze timing stats of completed jobs')
    p_analyze.set_defaults(func=main_analyze)
    p_analyze.add_argument('logfile', type=str, nargs='+', help='logfile(s) to analyze')

    return parser


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    random.seed()

    parser = arg_parser()
    args = parser.parse_args(argv)
    
    with open('config.yaml', 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    try:
        main_func = args.func
    except AttributeError:
        parser.print_help(file=sys.stderr)
        return 1

    return main_func(args, cfg)
    
    dir_cfg = cfg['directories']
    sched_cfg = cfg['scheduling']
    plotting_cfg = cfg['plotting']

    #
    # Stay alive, spawning plot jobs
    #
    #
    # Analysis of completed jobs
    #
    if True:
        # print('...scanning process tables')
        jobs = Job.get_running_jobs(dir_cfg['log'])

        # Status report
        if args.cmd == 'status':
            args.func(jobs)

        # Directories report
        elif args.cmd == 'dirs':
            (rows, columns) = os.popen('stty size', 'r').read().split()
            print(reporting.dirs_report(jobs, dir_cfg, sched_cfg, int(columns)))

        elif args.cmd == 'interactive':
            interactive.run_interactive()

        # Start running archival
        elif args.cmd == 'archive':
            print('...starting archive loop')
            firstit = True
            while True:
                if not firstit:
                    print('Sleeping 60s until next iteration...')
                    time.sleep(60)
                    jobs = Job.get_running_jobs(dir_cfg['log'])
                firstit = False
                archive.archive(dir_cfg, jobs)

        # Debugging: show the destination drive usage schedule
        elif args.cmd == 'dsched':
            dstdirs = dir_cfg['dst']
            for (d, ph) in manager.dstdirs_to_furthest_phase(jobs).items():
                print('  %s : %s' % (d, str(ph)))
        
        #
        # Job control commands
        #
        elif args.cmd in [ 'details', 'files', 'kill', 'suspend', 'resume' ]:
            print(args)

            selected = []

            # TODO: clean up treatment of wildcard
            if args.idprefix[0] == 'all':
                selected = jobs
            else:
                # TODO: allow multiple idprefixes, not just take the first
                selected = manager.select_jobs_by_partial_id(jobs, args.idprefix[0])
                if (len(selected) == 0):
                    print('Error: %s matched no jobs.' % id_spec)
                elif len(selected) > 1:
                    print('Error: "%s" matched multiple jobs:' % id_spec)
                    for j in selected:
                        print('  %s' % j.plot_id)
                    selected = []

            for job in selected:
                if args.cmd == 'details':
                    print(job.status_str_long())

                elif args.cmd == 'files':
                    temp_files = job.get_temp_files()
                    for f in temp_files:
                        print('  %s' % f)

                elif args.cmd == 'kill':
                    # First suspend so job doesn't create new files
                    print('Pausing PID %d, plot id %s' % (job.proc.pid, job.plot_id))
                    job.suspend()

                    temp_files = job.get_temp_files()
                    print('Will kill pid %d, plot id %s' % (job.proc.pid, job.plot_id))
                    print('Will delete %d temp files' % len(temp_files))
                    conf = input('Are you sure? ("y" to confirm): ')
                    if (conf != 'y'):
                        print('canceled.  If you wish to resume the job, do so manually.')
                    else:
                        print('killing...')
                        job.cancel()
                        print('cleaing up temp files...')
                        for f in temp_files:
                            os.remove(f)

                elif args.cmd == 'suspend':
                    print('Suspending ' + job.plot_id)
                    job.suspend()
                elif args.cmd == 'resume':
                    print('Resuming ' + job.plot_id)
                    job.resume()


def main_plot(args, cfg):
    dir_cfg = cfg['directories']
    sched_cfg = cfg['scheduling']
    plotting_cfg = cfg['plotting']
    print('...starting plot loop')
    while True:
        wait_reason = manager.maybe_start_new_plot(dir_cfg, sched_cfg, plotting_cfg)

        # TODO: report this via a channel that can be polled on demand, so we don't spam the console
        sleep_s = int(sched_cfg['polling_time_s'])
        if wait_reason:
            print('...sleeping %d s: %s' % (sleep_s, wait_reason))

        time.sleep(sleep_s)


def main_status(args, cfg):
    dir_cfg = cfg['directories']
    jobs = Job.get_running_jobs(dir_cfg['log'])
    (rows, columns) = os.popen('stty size', 'r').read().split()
    print(reporting.status_report(jobs, int(columns)))
    return 0


def main_analyze(args, cfg):
    analyzer = LogAnalyzer()
    analyzer.analyze(args.logfile)
    return 0


if __name__ == "__main__":
    sys.exit(main())