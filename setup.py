import os
from os import path
from setuptools import setup


local_path = os.path.dirname(__file__)
if not local_path:
    local_path = '.'
pwd = path.abspath(local_path)


def version():
    with open(pwd + '/xray/__version__.py', 'r') as ver:
        for line in ver.readlines():
            if line.startswith('version ='):
                return line.split(' = ')[-1].strip()[1:-1]
    raise ValueError('No version found in xray/version.py')


def read(fname):
    with open(fname, 'r') as filehandle:
        return filehandle.read()


def read_reqs(fname):
    req_path = os.path.join(pwd, fname)
    return [req.strip() for req in read(req_path).splitlines() if req.strip()]


# Get the long description from the README file
with open(path.join(pwd, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='xray',
    version=version(),
    description='Xray is a cli and api ready diff tool to compare differences '
                'between two structured datasets.',
    author='Aayush Jain',
    author_email='aayushj1811@gmail.com',
    license='BSD',
    # Note that this is a string of words separated by whitespace, not a list.
    keywords='pandas ETL',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/aayush-jain18/xray',
    packages=['xray'],
    install_requires=read_reqs('requirements.txt'),
    entry_points={'console_scripts': ['xray = xray.cli:xray']},
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Data Engineers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
    ],
)
