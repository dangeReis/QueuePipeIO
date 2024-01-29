from setuptools import setup, find_packages
import pypandoc

long_description = pypandoc.convert_file('README.md', 'rst')
setup(
    name='queuepipeio',  # Updated package name
    version='0.1.12',
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
        'Programming Language :: Python :: 3.11',
    ],
    long_description=long_description, 
    long_description_content_type='text/x-rst'
)