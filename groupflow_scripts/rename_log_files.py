#!/usr/bin/env python
import sys
from subprocess import *

if __name__ == '__main__':
    if len(sys.argv) >= 5:
        # Automated simulations - Differing link usage weights in Groupflow Module
        log_prefix = sys.argv[1]
        first_index = int(sys.argv[2])
        num_records = int(sys.argv[3])
        num_to_add = int(sys.argv[4])
        new_prefix = log_prefix
        if len(sys.argv) >= 6:
            new_prefix = sys.argv[5]
        
        for i in range(0, num_records):
            cmd = ['mv', log_prefix + str(i + first_index) + '.log', new_prefix + str(i + first_index + num_to_add) + '.log']
            print ' '.join(cmd)
            call(cmd)