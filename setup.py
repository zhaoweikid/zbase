#!coding: utf-8
import glob
from setuptools import setup, find_packages

setup(
    name="zbase",
    version="0.1",
    description="zhaowei's base module",
    author="zhaoweikid",
    url="http://code.pythonid.com",
    license="LGPL",
    packages= ["zbase", "zbase.db", "zbase.server"],
    package_dir = {"zbase":"./", "zbase.db":"db", "zbase.server":"server"}
)
