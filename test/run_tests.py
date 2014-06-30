# Monary - Copyright 2011-2013 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

"""
Note: This runs all of the tests. Maybe
      this will be removed eventually?
"""

import os
from os import listdir
from os.path import isfile, join
from inspect import getmembers, isfunction

def main():
    abspath = os.path.abspath(__file__)
    test_path = list(os.path.split(abspath))[:-1]
    test_path = join(*test_path)
    test_files = [ f.partition('.')[0] for f in listdir(test_path)
                   if isfile(join(test_path,f)) and f.endswith('.py')]
    test_files = filter(lambda f: f.startswith('test'), test_files)
    for f in test_files:
        print 'Running tests from', f
        exec('import ' + f + ' as test')
        methods = [ m[0] for m in getmembers(test, isfunction) ]
        if 'setup' in methods:
            test.setup()
        for m in filter(lambda m: m.startswith('test'), methods):
            getattr(test, m)()
        if 'teardown' in methods:
            test.teardown()

if __name__ == '__main__':
    main()
