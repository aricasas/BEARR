from math import *

def monkey(m_0, level, t):
    return ceil(max(0, m_0 - (level * log2(t) / log(2))))


def monkey_total_bits_used(memtable_size, n_entries, bits_top_level, size_ratio):
    n_count = n_entries
    level = 0
    bits_used = 0
    while n_count > 0:
        for _ in range(size_ratio):
            n_in_sst = min(memtable_size * size_ratio**level, n_count)
            bits_used += n_in_sst * monkey(bits_top_level, level, size_ratio)
            n_count -= n_in_sst
            if n_count <= 0:
                break
        level += 1

    return bits_used

def uniform_total_bits_used(n_entries, bits_top_level):
    return n_entries * bits_top_level

p = 655360
n = 64 * 1024 * 1024
t = 4
b = 8
mb = 13

u = uniform_total_bits_used(n, b)
monkey = monkey_total_bits_used(p, n, mb, t)

print("Uniform 1 GiB db with " + str(b) + " bits uses " + str(u / (1024*1024)) + " MiB")
print("Monkey 1 GiB db with " + str(mb) + " bits uses in worst possible case " + str(monkey/ (1024*1024)) + " MiB")
