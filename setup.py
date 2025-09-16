from setuptools import setup

setup(
    name='nameko-pymemcache',
    version='0.1.0',
    url='https://github.com/andreasmyleus/nameko-pymemcache/',
    license='Apache License, Version 2.0',
    author='andreasmyleus',
    author_email='andreas@pdc.ax',
    py_modules=['nameko_pymemcache'],
    install_requires=[
        "nameko>=2.0.0",
        "pymemcache>=4.0.0",
    ],
    description='Memcached dependency for nameko services',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
