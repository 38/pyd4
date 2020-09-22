import pyd4
import re

file = pyd4.D4File("../data/hg002.d4")

#regions = list(filter(lambda x: len(x) < 6, map(lambda a: a[0], file.chroms())))

regions = []

for name, size in file.chroms():
    if re.match(r"^chr[0-9XY]*$", name):
        begin = 0
        while begin < size:
            end = min(begin + 10000, size)
            regions.append((name, begin, end))
            begin = end

for (mean, r) in zip(file.mean(regions), regions):
    print(r[0], r[1], r[2], mean)
