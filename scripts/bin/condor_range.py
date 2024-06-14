# Author: Panitan Wongse-ammat
#
# Objective: This script is created to run condor commands
#   in a range of jobids.
#

import os
import argparse
import git
import logging as log


def build_arg_parser():
    args = argparse.ArgumentParser(
        description="generate the experiment script"
    )

    args.add_argument(
        "-s",
        "--starting-jobid",
        action="store",
        help="the starting jobid to remove from the condor",
        type=int,
        default=0,
        dest="starting_jobid",
        required=False
    )

    args.add_argument(
        "-e",
        "--ending-jobid",
        action="store",
        help="the ending jobid to remove from the condor inclusive",
        type=int,
        default=0,
        dest="ending_jobid",
        required=False
    )

    args.add_argument(
        "-t",
        "--testing-cmd",
        action="store_true",
        help="print out the commands without running them",
        default=False,
        dest="testing_cmd",
        required=False
    )

    args.add_argument(
        "-c",
        "--condor-cmd",
        action="store",
        help=
        "the condor command by default the value of this variable is condor_rm",
        default="condor_rm",
        dest="condor_cmd",
        required=False
    )

    args.add_argument(
        "-d",
        action="store_true",
        help="debugging this script and print out all debugging messages",
        dest="debug",
        default=False,
    )

    return args


def main():
    arg_parser = build_arg_parser()
    args = arg_parser.parse_args()

    # warning 30, info 20, debug 10
    if args.debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO

    log.basicConfig(
        format=
        "[%(levelname)s m:%(module)s f:%(funcName)s l:%(lineno)s]: %(message)s",
        level=log_level
    )

    log.debug("starting_jobid: %d", args.starting_jobid)
    log.debug("ending_jobid: %d", args.ending_jobid)

    assert args.starting_jobid < args.ending_jobid, \
        "the starting jobid %d should be less than the ending jobid %d" \
            % (args.starting_jobid, args.ending_jobid)

    for jobid in range(args.starting_jobid, args.ending_jobid + 1, 1):
        cmd = "{} {}".format(args.condor_cmd, jobid)
        log.info("running: %s", cmd)
        if not args.testing_cmd:
            os.system(cmd)


if __name__ == '__main__':
    main()
