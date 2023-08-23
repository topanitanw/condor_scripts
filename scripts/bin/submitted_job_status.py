import htcondor
import pandas as pd

status_string = {}
for member in htcondor.JobStatus:
    status_string[member.value] = member.name

# Connect to the HTCondor collector
coll = htcondor.Collector()

# Get a list of jobs from the collector
schedd_ads = coll.locate(htcondor.DaemonTypes.Schedd)
schedd = htcondor.Schedd(schedd_ads)
jobs = schedd.query()

# Create a list to store job data
job_data = []

# Collect job status data
for job in jobs:
    # print(job)
    cluster_id = job.get("ClusterId")
    proc_id = job.get("ProcId")
    status = job.get("JobStatus")
    owner = job.get("Owner")
    remote_host = job.get("RemoteHost", "None")
    remote_host = remote_host.split(".")[0]

    if cluster_id is not None and proc_id is not None and status is not None:
        # print(cluster_id, proc_id, status)
        status_str = status_string[status]
        job_data.append({
            "ClusterId": cluster_id,
            "ProcId": proc_id,
            "Status": status_str,
            "Owner": owner,
            "RemoteHost": remote_host,
        })
    else:
        print("Invalid job data:", job)

# Create a DataFrame
df = pd.DataFrame(job_data)

# Sort the DataFrame based on the "foo" column
df = df.sort_values(by="Owner", ignore_index=True)

# Print the DataFrame to stdout
print(df)
