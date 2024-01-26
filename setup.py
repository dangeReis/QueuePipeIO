from setuptools import setup, find_packages

setup(
    name='queuebytesio',
    version='0.1.1',
    description='A project that provides queue-based I/O functionality',
    packages=find_packages(),
    install_requires=[
        'boto3',
        'zstandard',
        'tqdm'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
    ],
)