import subprocess
import pandas as pd


def get_condor_slots():
    try:
        # Run the condor_status command and capture its output
        result = subprocess.run(
            ['condor_status'],
            stdout=subprocess.PIPE,
            text=True,
            check=True,
        )

        # Split the output into lines
        lines = result.stdout.split('\n')

        # Parse the output to extract available slots per server
        slots = {}
        line_num = 0
        for line in lines:
            if line_num == 0:
                line_num += 1
                continue

            if line.strip():
                fields = line.split()

                if len(fields) < 5:
                    continue
                # fields[0] is 'slot4@allagash.cs.northwestern.edu'
                # server = allagash
                # print(fields)

                if "slot" in fields[0]:
                    server = fields[0].split('@')[1].split('.')[0]

                    server_info = slots.get(
                        server,
                        {
                            'total': 0,
                            'available': 0
                        },
                    )
                    if fields[3] == 'Unclaimed':
                        server_info['available'] += 1
                    server_info['total'] += 1
                    slots[server] = server_info

        return slots

    except subprocess.CalledProcessError as e:
        print("Error running condor_status:", e)
        return {}


if __name__ == "__main__":
    available_slots = get_condor_slots()

    # create the data frame here
    # there is a header that machine, available, total, % available
    df = pd.DataFrame(columns=['machine', 'available', 'total', '% available'])

    total_available_slots = 0
    for machine, slot_info in available_slots.items():
        total_slots = slot_info['total']
        available_slots = slot_info['available']
        total_available_slots += available_slots
        # if I want to put the data in the data frame, without using append
        percent_available = available_slots / total_slots * 100
        df.loc[len(df)] = [
            machine,
            available_slots,
            total_slots,
            percent_available,
        ]

    # sort the data frame by % available
    df = df.sort_values(by=['% available'], ascending=False)
    print(df)
    print(f"Total Available Slots: {total_available_slots}")
