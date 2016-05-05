import subprocess
from setuptools import setup, find_packages, Extension

setup(
        name='pgbquery',
        version='0.0.4',
        author='Rick Albright',
        license='BSD',
        description='A query and aggregation framework for Bcolz and Postgresql',
	long_description="""\
	  Bcolz is a light weight package that provides columnar, chunked data 
	  containers that can be compressed either in-memory and on-disk. 
	  that are compressed by default not only for reducing memory/disk 
	  storage, but also to improve I/O speed. It excels at storing and 
	  sequentially accessing large, numerical data sets.
	  
	  Pgbquery is largely based on the bquery framework which can be 
	  found on github at https://github.com/visualfabriq/bquery, with 
	  changes made to support the multicorn foreign data wrapper.

	  The pgbquery framework provides methods to perform query and aggregation 
	  operations on bcolz containers, a multicorn wrapper, as well as accelerate 
	  these operations by pre-processing possible groupby columns. Currently the 
	  real-life performance of sum aggregations using on-disk bcolz queries 
	  is normally between 1.5 and 3.0 times slower than similar in-memory 
	  Pandas aggregations.

    """,
        install_requires=['bcolz', 'bquery', 'multicorn'],
        packages=find_packages()
)
