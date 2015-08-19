try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'My Project',
    'author': 'Francis Tan',
    'url': 'URL to get it at.',
    'download_url': 'Where to download it.',
    'author_email': 'ftan@discoverydn.com',
    'version': '0.1',
    'install_requires': ['MySQL-python', 'pytest'],
    'packages': ['ssd_utilities'],
    'scripts': [],
    'name': 'ssd-reporting'
    }

setup(**config)
