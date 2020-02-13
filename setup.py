#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup, Extension
import os
import platform

ignored_files = ['rwrapper.cpp']
extensions = ['.cpp', '.cc']
include_paths = ['src', 'src/thrift']
toolchain_args = ['-std=c++11']
if platform.system() == 'Darwin':
    toolchain_args.extend(['-stdlib=libc++', '-mmacosx-version-min=10.7'])

def get_files(dirname):
	file_list = os.listdir(dirname)
	result = []
	for fname in file_list:
		if fname in ignored_files:
			continue
		full_name = os.path.join(dirname, fname)
		if os.path.isdir(full_name):
			result += get_files(full_name)
		else:
			if full_name.endswith('.cpp') or full_name.endswith('.cc'):
				result.append(full_name)
	return result

libminiparquet = Extension('miniparquet',
                    sources = get_files('src'),
					include_dirs = include_paths,
					extra_compile_args=toolchain_args,
					extra_link_args=toolchain_args,
					language = 'c++')

setup (name = 'miniparquet',
       version = '0.1',
       description = 'Miniparquet',
	   install_requires=[ # these versions are still available for Python 2, newer ones aren't
			'numpy>=1.14'
		],
       ext_modules = [libminiparquet])
