# Monary - Copyright 2011-2014 David J. C. Beach
# Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

from distutils.core import setup, Extension

VERSION = "0.3.0"

# Get README info
try:
    with open("README.rst") as fd:
        readme_content = fd.read()
except:
    readme_content = ""

setup(
    name = "Monary",
    version = VERSION,
    packages = ["monary"],
    requires = ["pymongo", "numpy"],
    
    package_dir = {"monary": "monary"},
    ext_modules = [Extension("cmonary",
                             ["monary/cmonary.c", "monary/cmonary.h"])],

    author = "David J. C. Beach",
    author_email = "info@djcinnovations.com",
    description = "Monary performs high-performance column queries from MongoDB.",
    long_description = readme_content,
    keywords = "monary pymongo mongo mongodb numpy array",
    classifiers = [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Operating System :: Microsoft :: Windows",
        "Environment :: Console",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database"
    ],
    url = "http://bitbucket.org/djcbeach/monary/"
)
