import pandas as pd
import numpy as np
import sys
import json

def ms2hr(ms_val):
    return ms_val/(1000*60*60)

def file2df(stream_file_list):
    dfs = []

    for f_name in stream_file_list:
        with open(f_name) as f:
            df_from_json = pd.json_normalize(json.loads(f.read()))
            dfs.append(df_from_json)

    df = pd.concat(dfs, sort=False)
    return df


def top_taylor(df, top=12):
    df_ts=df[df['master_metadata_album_artist_name'] == "Taylor Swift"]
    conditions = [
        (df_ts['master_metadata_album_album_name'].str.contains("Taylor Swift")),
        (df_ts['master_metadata_album_album_name'].str.contains("Fearless")),
        (df_ts['master_metadata_album_album_name'].str.contains("Speak Now")),
        (df_ts['master_metadata_album_album_name'].str.contains("Red")),
        (df_ts['master_metadata_album_album_name'].str.contains("1989")),
        (df_ts['master_metadata_album_album_name'].str.contains("reputation")),
        (df_ts['master_metadata_album_album_name'].str.contains("Lover")),
        (df_ts['master_metadata_album_album_name'].str.contains("folklore")),
        (df_ts['master_metadata_album_album_name'].str.contains("evermore")),
        (df_ts['master_metadata_album_album_name'].str.contains("Midnights")),
        (df_ts['master_metadata_album_album_name'].str.contains("TORTURED POETS"))
        ]
    choices = ["Taylor Swift","Fearless", "Speak Now", "Red", "1989", "reputation", "Lover", "folklore", "evermore", "Midnights", "THE TORTURED POETS DEPARTMENT"]
    df_ts['albumName'] = np.select(conditions,choices, default="Other")

    df_top = df_ts.groupby(['albumName'], as_index=False) \
        .agg({'ts':'count', 'ms_played':'sum'}) \
        .rename(columns={'ts':'noStreams', 'ms_played':'streamTimeMs'})
    df_top['streamTimeHr'] = ms2hr(df_top['streamTimeMs'])
    df_top = df_top.sort_values(by=['noStreams'], ascending=False)
    df_top = df_top.head(top)

    print(df_top)
    return df_top


def main(stream_file_list):
    df = file2df(stream_file_list)
    top_taylor(df)


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        main(sys.argv[1:])