from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(
	name='search_ads_two',
	version='0.1',
	description='Interfacing with Apple Search Ads API',
	url='http://github.com/migueltheasoco/search_ads',
	author='Miguel Carvalho',
	author_email='miguel@theaso.co',
	license='MIT',
	packages=['search_ads_two'],
	test_suite='nose.collector',
	tests_require=['nose'],
	install_requires=[
	'matplotlib',
	'gspread',
	'ipdb',
	'numpy',
	'pandas',
	'requests',
	#'google.cloud',
	'oauth2client',
	],
    zip_safe=False)