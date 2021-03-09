import argparse
import math
import re
import os
import sys

import pandas as pd
import tabulate
from tabulate import tabulate

from models.utils import chunks, fmt_tag

PREFIX_FILTER = "league_stats"

ap = argparse.ArgumentParser(description="Clan stats")
ap.add_argument("--clan-tag", default=None)
ap.add_argument("--clan-name", default=None)
ap.add_argument("--d-no-tag", default=None)
ap.add_argument("--f-nattacks", default=None)
ap.add_argument("--drop-col", default=None)
ap.add_argument("--sort-by", default=None)
ap.add_argument("--format", default=None)
ap.add_argument("league_file")
# ap.add_argument('rest', nargs='*', type=int)
args = ap.parse_intermixed_args()

files = []
f = os.path.abspath(args.league_file)
if os.path.exists(f) and os.path.isfile(f):
    files.append(f)
elif os.path.isdir(f):
    files = [os.path.join(f, x) for x in os.listdir(f)]
else:
    raise IOError(f"File or Directory not found '{f}'")

dfs = []
for f in files:
    bn = os.path.basename(f)
    if not bn.startswith(PREFIX_FILTER):
        continue
    df = pd.read_csv(f)

    if args.clan_tag:
        ct = fmt_tag(args.clan_tag)
        df = df[df["clan_tag"] == ct]
        dfs.append(df)
    elif args.clan_name:
        r = f".*{args.clan_name}.*"
        df = df[df["clan"].str.contains(args.clan_name, regex=False)]
        dfs.append(df)
if len(dfs) == 0:
    s = args.clan_name if args.clan_name else args.clan_tag
    raise IOError(f"There were no clans matching '{s}'")
df = pd.concat(dfs)


drop_cols = [
    "d_counts",
    "d_rank_stars",
    "d_percentile_stars",
    "attacker_tag",
    "defender_tag",
    "d_total_stars",
    "d_d%_mean",
    "a_rank_dp",
    "d_rank_dp",
    "d_percentile_dp",
    "a_percentile_dp",
    "clan_tag"
]
df.rename(
    columns={
        "a_counts": "#",
        "a_total_stars": "⭐ (sum)",
        "a_stars_mean": "⭐ (avg)",
        "a_d%_mean": "% (avg)",
        "d_stars_mean": "D⭐ (avg)",
        "a_rank_stars": "rank",
        "a_percentile_stars": "percentile",
    },
    inplace=True,
)
if args.sort_by:
    cols = [
        args.sort_by,
        "⭐ (sum)",
        "⭐ (avg)",
        "% (avg)",
        "d_total_stars",
        "D⭐ (avg)",
        "d_d%_mean",
    ],
    ascending=[True, True, True, True, False, False, False]
    print(cols, ascending)
    df.sort_values(*cols,ascending=ascending, inplace=True)

df = df.drop(drop_cols, axis=1)
if args.f_nattacks:
    df = df[df["#"] >= int(args.f_nattacks)]
if args.drop_col:
    cols = args.drop_col.split(",")
    for c in cols:
        df.drop(c.strip(), axis=1, inplace=True)

CHUNK_SIZE = 12

table = tabulate(df.values, headers=df.columns, floatfmt=".2f")
lstr = str(table)
if args.format == "discord":
    lines = lstr.split("\n")
    for ch in chunks(lines, CHUNK_SIZE):
        print("```")
        print("\n".join(ch))
        print("```")
else:
    print(table)