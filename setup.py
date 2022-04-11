#!/usr/bin/env python3

from setuptools import setup
from setuptools import find_packages

setup(name="spark_structured_streaming_consumer",
      version="1.0.0",
      packages=find_packages(exclude=['query_templates']),
      package_dir={'libs': 'libs', 'helpers': 'helpers'},
      zip_safe=False,
      data_files=[(
          'project_configs/configs', [
              './project_configs/general_config.yaml', 
              './project_configs/topics_config.yaml',
              './project_configs/consumer_config.yaml']
      )],
      python_requires='>=3.7',
      description="spark structured streaming consumer",
      author="marssu",
      author_email="mars.su@gogolook.com",
      license="MIT",
      platforms="Independant")
