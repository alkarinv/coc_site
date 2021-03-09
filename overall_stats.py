import sys

import pandas as pd
import tabulate
from tabulate import tabulate

df = pd.read_csv("league_summary.csv")
print(df)
drop = [x for x in df.columns if "percent" not in x]
drop.remove("league")
df["league"] = df["league"].astype("Int64")

# df.rename(columns=
# {
#     'a_count': '#',
#     'a_total_stars': '⭐ (total)',
#     'a_stars_mean': '⭐ (avg)',
#     'a_d%_mean': '% (avg)',
#     'd_stars_mean': 'D⭐ (avg)',
#     'a_rank_stars': 'rank',
#     'a_percentile_stars': 'percentile'
# }, inplace=True)
df = df.drop(drop, axis=1)

CHUNK_SIZE = 5
def to_discord(df):
    chunks = int(df.shape[0]/CHUNK_SIZE) + 1
    table = tabulate(df.values, headers=df.columns, floatfmt=".2f")
    lstr = str(table)
    lines = lstr.split('\n')
    print(df)
    print(lines)
    print(len(lines), df.shape[0])
    for i in range(0, chunks):
        print("```")
        print('\n'.join(lines[i*CHUNK_SIZE:i*CHUNK_SIZE+CHUNK_SIZE]))
        print("```")

to_discord(df)
