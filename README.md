mongo-monary-driver
===================
Blazingly-fast reads on MongoDB.

About
-----
Monary is a simple library for interacting with
[MongoDB](http://www.mongodb.org) in Python with performance in mind.

This version of Monary is inspired by the original
[Monary](https://bitbucket.org/djcbeach/monary), written by [David J. C.
Beach](http://djcinnovations.com/).

The original Monary is in beta and is being actively developed. This version is
highly experimental and is not intended as a replacement. Do not use in
production!

Rationale
---------
For everyday use of MongoDB via Python,
[PyMongo](https://pypi.python.org/pypi/pymongo/) is the tool of choice. It
retrieves MongoDB documents as Python dictionaries and has a host of other tools
for interacting with BSON and gridfs. However, for certain applications, it may
be more useful to have data in array or list format, which lends itself to easy
data manipulation. To obtain data in this format with PyMongo is cumbersome -
dictionaries are populated with data, which are discarded after transferring all
of the data into lists.

To achieve fast queries, Monary uses the [MongoDB C
Driver](https://github.com/mongodb/mongo-c-driver) to perform a collection find.
Then, it uses the [Python C API](https://docs.python.org/2/c-api) to create a
Python list that stores the results.

Support/Feedback
----------------
Although there are no plans to actively develop this, feel free to add an issue
or submit a pull request if you'd like. If you actually want to use Monary, you
can download the official version [on PyPI](https://pypi.python.org/pypi/Monary)
and see the original source [on
BitBucket](https://bitbucket.org/djcbeach/monary).

Installation
------------
It is highly suggested to install this only locally or in a virtualenv and not
system-wide.

Run the following command from the source directory to install:

    $ python setup.py install

As Monary is a Python C extension, it requires compilers to be installed, as
well as the Python development headers. If you don't have these, you can install
them on your system with your local package manager.

On Ubuntu and Debian:

    $ sudo apt-get install build-essential python-dev

For Red Hat-based distributions (RHEL, CentOS, etc.):

    $ sudo yum install gcc python-devel

On Arch Linux, development headers should already be included in the regular
Python distribution:

    $ sudo pacman -S base-devel gcc python2
