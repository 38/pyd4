"""
The Python Binding for the D4 file format
"""

from .pyd4 import D4File, D4Iter

def enumerate_values(inf, chrom, begin, end):
    return map(lambda p: (chrom, p[0], p[1]), zip(range(begin, end), inf.value_iter(chrom, begin, end)))

__all__ = [ 'D4File', 'D4Iter' ]
