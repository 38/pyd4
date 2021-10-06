"""
The Python Binding for the D4 file format
"""

from .pyd4 import D4File, D4Iter

def enumerate_values(inf, chrom, begin, end):
    if inf.__class__ == list:
        def gen():
            iters = [x.value_iter(chrom, begin, end) for x in inf]
            for pos in range(begin, end):
                yield (chrom, pos, [f.__next__() for f in iters])
        return gen()
    return map(lambda p: (chrom, p[0], p[1]), zip(range(begin, end), inf.value_iter(chrom, begin, end)))

def open_all_tracks(fp):
    f = D4File(fp)
    return [f.open_track(track_label) for track_label in f.list_tracks()]

__all__ = [ 'D4File', 'D4Iter' ]
