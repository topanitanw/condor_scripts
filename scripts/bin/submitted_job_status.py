import sys
import htcondor
import argparse
import logging as log
import pandas as pd
import os
import re

status_string = {}
for member in htcondor.JobStatus:
    status_string[member.value] = member.name


def parse_condor_status(args_cl):

    # Connect to the HTCondor collector
    coll = htcondor.Collector()

    # Get a list of jobs from the collector
    schedd_ads = coll.locate(htcondor.DaemonTypes.Schedd)
    schedd = htcondor.Schedd(schedd_ads)
    jobs = schedd.query()

    # Create a list to store job data
    job_data = []

    username = os.getlogin()
    # Collect job status data
    index = 0
    for job in jobs:
        owner = job.get("Owner", "")
        if args_cl.user_only and owner != username:
            continue

        index += 1
        if index < 1:
            requirement = job.get("Requirements", "")
            print("requirement:", requirement)
        # cluster_id = job.get("ClusterId")
        # proc_id = job.get("ProcId")
        # print(cluster_id, proc_id)
        # if cluster_id != "1912416":
        #     continue
        # exit(123)
        cluster_id = job.get("ClusterId", "")
        proc_id = job.get("ProcId", "")

        status = job.get("JobStatus")
        owner = job.get("Owner", "")

        remote_host = job.get("RemoteHost", "None")
        if remote_host != "None":
            channel_remote_host = remote_host.split(".")[0]
            remote_host = channel_remote_host.split("@")[1]

        cluster_id = job.get("ClusterId", "")
        proc_id = job.get("ProcId", "")
        # args = job.get("Args", "")
        is_whole_machine_job = job.get("IsWholeMachineJob", False)

        requirement = job.get("Requirements", "")
        log.debug("requirement: %s", requirement)
        log.debug("type: %s", type(requirement))

        pattern = r'(\w+).cs.northwestern.edu'
        matches = re.findall(pattern, str(requirement))
        nmachine_require = len(matches)
        target_machine = ""
        if nmachine_require > 0:
            target_machine = matches[0]

        if cluster_id is not None \
            and proc_id is not None \
            and status is not None:
            # print(cluster_id, proc_id, status)
            status_str = status_string[status]
            job_data.append(
                {
                    "ClusterId": cluster_id,
                    "ProcId": proc_id,
                    "Status": status_str,
                    "Owner": owner,
                    "RemoteHost": remote_host,
                    "Args": "",  # args,
                    "IsWholeMachineJob": is_whole_machine_job,
                    "NTargetMachine": nmachine_require,
                    "TargetMachine": target_machine,
                },
            )
        else:
            print("Invalid job data:", job)

    column = [
        "ClusterId",
        "ProcId",
        "Status",
        "Owner",
        "RemoteHost",
        "Args",
        "IsWholeMachineJob",
        "NTargetMachine",
        "TargetMachine",
    ]

    # Create a DataFrame
    df = pd.DataFrame(job_data, columns=column)

    # Sort the DataFrame based on the "foo" column
    df = df.sort_values(by="Owner", ignore_index=True)

    # Print the DataFrame to stdout

    df.to_csv("status.csv", index=False)

    return df


def process_data(df):
    server_data = {}
    for _, row in df.iterrows():
        server_name = row["RemoteHost"]
        if server_name != "None":
            server_dict = server_data.get(
                server_name,
                {
                    "IsWholeMachineJob": False,
                    "NumUsedSlot": 0,
                    "nqueue": 0,
                    "user": "",
                },
            )
            server_dict["NumUsedSlot"] += 1
            is_whole_machine_job = row["IsWholeMachineJob"]
            server_dict["IsWholeMachineJob"] |= is_whole_machine_job
            current_user = server_dict["user"]
            owner = row["Owner"]
            if owner not in current_user:
                if current_user:
                    owner = "," + owner
                server_dict["user"] += owner
        else:
            server_name = row["TargetMachine"]
            server_dict = server_data.get(
                server_name,
                {
                    "IsWholeMachineJob": False,
                    "NumUsedSlot": 0,
                    "nqueue": 0,
                    "user": "",
                },
            )

            if row["NTargetMachine"] > 1:
                continue

            server_dict["nqueue"] += 1

        server_data[server_name] = server_dict
    df = pd.DataFrame.from_dict(server_data, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'server'}, inplace=True)
    df = df.sort_values(by=["server"])
    df = df.reset_index(drop=True)
    return df


def build_arg_parser():
    args = argparse.ArgumentParser(
        description="generate the experiment script"
    )

    args.add_argument(
        "-m",
        "--me",
        action="store_true",
        help="Print only the information related to the user",
        default=False,
        dest="user_only",
        required=False,
    )
    args.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="debugging this script and print out all debugging messages",
        dest="debug",
        default=False,
    )

    return args


def main():
    arg_parser = build_arg_parser()
    args_cl = arg_parser.parse_args()

    # warning 30, info 20, debug 10
    log_level = log.INFO
    if args_cl.debug:
        log_level = log.DEBUG

    mesg_prefix = "[%(levelname)s m:%(module)s f:%(funcName)s l:%(lineno)s]:"
    mesg_body = " %(message)s"
    mesg_format = mesg_prefix + mesg_body
    log.basicConfig(
        format=mesg_format,
        level=log_level,
    )

    raw_df = parse_condor_status(args_cl)
    processed_df = process_data(raw_df)

    if processed_df.empty:
        print("No job is running.")
        sys.exit(0)

    print(processed_df)


if __name__ == '__main__':
    main()
