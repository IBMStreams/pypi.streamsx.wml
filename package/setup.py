from setuptools import setup
import streamsx.wml
setup(
  name = 'streamsx.wml',
  packages = ['streamsx.wml'],
  include_package_data=True,
  version = streamsx.wml.__version__,
  description = 'Watson Machine Learning online scoring integration in IBM Streams topology applications',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'hegermar@de.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/pypi.streamsx.wml',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'wml', 'streamsx' , 'topology'],
  classifiers = [
    'Development Status :: 1 - Planning',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
  install_requires=['streamsx>=1.13.15'],
  
  test_suite='nose.collector',
  tests_require=['nose']
)
