from setuptools import setup, find_packages

setup(
    name='QueueBytesIO',
    version='0.1.0',
    url='https://github.com/dangeReis/QueueBytesIO',
    description='A queue-based IO stream for Python',
    packages=find_packages(),    
    install_requires=['boto3', 'zstandard', 'tqdm'],
)