from setuptools import setup, find_packages

version = '2.0.0'

setup(name='switchboard',
      version=version,
      description="Feature flipper for Pyramid, Pylons, or TurboGears apps.",
      # http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=[
          "Programming Language :: Python",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='switches feature flipper pyramid pylons turbogears',
      author='Kyle Adams',
      author_email='kadams54@users.noreply.github.com',
      url='https://github.com/switchboardpy/switchboard/',
      download_url='https://github.com/switchboardpy/switchboard/releases',
      license='Apache License',
      packages=find_packages(exclude=['ez_setup']),
      include_package_data=True,
      install_requires=[
          'blinker>=1.2',
          'bottle== 0.12.8',
          'Mako>=0.9',
          'SQLAlchemy>=1.2.4',
          'WebOb>=0.9',
      ],
      zip_safe=False,
      tests_require=[
          'nose',
          'mock',
          'paste',
          'selenium',
          'splinter',
      ],
      test_suite='nose.collector',
      )
