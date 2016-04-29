from __future__ import division

import sys
import os
import collections
import numpy as np
import math

def load_matrix(fname, sz):
    M = np.matrix(np.zeros(sz))
    with open(fname) as f:
        content = f.readlines()
        for l in content:
            s = l.split()
            M[int(s[0]), int(s[1])] = float(s[2])
    return M

A = load_matrix("/Users/roshaninagmote/Downloads/a2_graphs/Assign2_100_new.txt", (100, 100))
B = load_matrix("/Users/roshaninagmote/Downloads/a2_graphs/Assign2_100_new.txt", (100, 100))

C = A*B
print C[38][0]