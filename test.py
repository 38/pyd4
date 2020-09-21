import pyd4

file = pyd4.D4File("../data/hg002.d4")

regions = list(filter(lambda x: len(x) < 6, map(lambda a: a[0], file.chroms())))

for ((hist, _, _), r) in zip(file.histogram(regions, 0, 10000), regions):
    sum = 0
    count = 0
    for val, cnt in hist:
        sum += val * cnt
        count += cnt
    print(r, sum / count)
