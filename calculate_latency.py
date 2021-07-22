from datetime import datetime
import pandas as pd
import sys
import traceback
import pdb

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("usage: python {} saveto.h5".format(sys.argv[0]))
        sys.exit()
    logbody = {}
    with open("log.txt") as f:
        all_log = f.readlines()
    print(all_log[0].split(","))
    for i in all_log:
        # print(i)
        try:
            ts, body = i.split(",")[0:2]
        except:
            pdb.set_trace()
        ts = ts.strip()
        ts_sending, tid = body.split("|")[0:2]
        tid = tid.strip()
        ts_sending = ts_sending.strip()
        if tid not in logbody:
            logbody[tid] = (ts, ts_sending)
    # print(logbody)
    with open("D:/testworkspace/go_request_result.txt") as f:
        trl = [line.strip().split(",") for line in f]
        trl_result = [(x[0], datetime.fromtimestamp(float(x[1]) / 1e9)) for x in trl]
        trl_df = pd.DataFrame(trl_result, columns=["id", "start_time"])

    for i, row in trl_df.iterrows():
        try:
            endorsements_time = datetime.fromtimestamp(float(logbody[row["id"]][0]))
            sending_time = datetime.fromtimestamp(float(logbody[row["id"]][1])/1e3)
            trl_df.at[i, 'endorsements_time'] = endorsements_time
            trl_df.at[i, 'takes'] = endorsements_time - row["start_time"]
            trl_df.at[i, 'start_to_sending'] = sending_time - row["start_time"]
            trl_df.at[i, 'sending_to_ending'] = endorsements_time - sending_time
        except Exception:
            print(traceback.format_exc())
            pdb.set_trace()
    print(trl_df["start_to_sending"].mean().total_seconds())
    print(trl_df["sending_to_ending"].mean().total_seconds())
    print(trl_df["takes"].mean().total_seconds())
    print(trl_df["takes"].median().total_seconds())
    print(trl_df["takes"].quantile(0.5).total_seconds())
    print(trl_df["takes"].min().total_seconds())
    print(trl_df["takes"].max().total_seconds())
    store = pd.HDFStore(sys.argv[1])
    store.put("transaction_record_df", trl_df)
    store.close()
