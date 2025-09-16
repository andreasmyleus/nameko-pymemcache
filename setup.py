from setuptools import setup

# Read the README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='nameko-pymemcache',
    use_scm_version=True,  # Automatically get version from Git tags
    url='https://github.com/andreasmyleus/nameko-pymemcache/',
    license='Apache License, Version 2.0',
    author='andreasmyleus',
    author_email='andreas@pdc.ax',
    py_modules=['nameko_pymemcache'],
    setup_requires=['setuptools_scm'],
    install_requires=[
        "nameko>=2.0.0",
        "pymemcache>=4.0.0",
    ],
    description='Memcached dependency for nameko services with consistent hashing',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Topic :: System :: Distributed Computing',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    python_requires='>=3.8',
    keywords='nameko memcached cache distributed consistent-hashing',
)
