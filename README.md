# Python streamsx.wml package

This package provides the online scoring functionality of Watson Machine Learning in IBM Cloud Pak for Data for streaming applications build with streamsx package.


Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way, but package must be prepared to `ant build` before uploading it:
```
ant build
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.wml

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```

or

    ant doc


and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxwml.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/wml/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/wml/\_\_init\_\_.py
- ./package/DESC.txt


## Prepare package for release

This step must be done before package is uploaded to pypi.org:

```
ant build
```


## Test

Package can be tested with TopologyTester.

Launch the test cases for build only verification (streamsx.topology.context.ContextTypes: TOOLKIT and BUNDLE):

```
cd package
python3 -u -m unittest streamsx.wml.tests.test_wml.Test
```

or

    ant test
