#!/usr/bin/env python

import click
import pandas as pd
import numpy as np
from collections import OrderedDict
from Ranger import RangeSet
from Ranger import Range


GERMLINE_COLUMNS=[
    "Fam1",
    "Id1",
    "Fam2",
    "Id2",
    "Chr",
    "SegStart",
    "SegEnd",
    "SnpStart",
    "SnpEnd",
    "NoSnp",
    "Length",
    "Unit",
    "NoMismatch",
    "Res1",
    "Res2"
]

def merge_segments_for_chr(s):
    merged = RangeSet([Range.closed(*t) for t in zip(s.SegStart, s.SegEnd)])
    seg_start = np.array([x.lowerEndpoint() for x in merged.ranges])
    seg_end = np.array([x.upperEndpoint() for x in merged.ranges])
    seg_no = len(seg_start)
    return pd.DataFrame.from_dict(OrderedDict([
        ('SegStart', seg_start),
        ('SegEnd', seg_end),
        ('SnpStart', seg_start),
        ('SnpEnd', seg_end),        
        ('NoSnp', np.zeros(seg_no,dtype=int)),
        ('Length', (seg_end-seg_start)), 
        ('NoSnp', np.zeros(seg_no,dtype=int)),
        ('Unit', np.repeat('bp', seg_no)),
        ('NoMismatch', np.zeros(seg_no,dtype=int)),
        ('Res1', np.zeros(seg_no,dtype=int)),
        ('Res2', np.zeros(seg_no,dtype=int)), 
        ('Fam1',np.repeat('-', seg_no)),
        ('Fam2',np.repeat('-', seg_no)),
    ]))


@click.command()
@click.argument('input_seg', required=True)
@click.argument('output_seg', required=True)
def merge_segments(input_seg, output_seg):
   df_segments = pd.read_table(input_seg, names=GERMLINE_COLUMNS)
   df_segments_merged =  df_segments.groupby(['Id1','Id2', 'Chr']).apply(merge_segments_for_chr)
   df_results = df_segments_merged.reset_index()[GERMLINE_COLUMNS]
   df_results.to_csv(output_seg, header=False, sep='\t', index=False, compression='gzip')

if __name__ == '__main__':
    merge_segments()