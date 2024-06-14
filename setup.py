from setuptools import find_packages, setup

setup(
    name='src',
    packages=find_packages(),
    version='0.0.1',
    description='This is a Topic Modelling Software. For an input of a daily sequence of a set of tweets (or text), it (1) Detects topics from tweets (text) and (2) scores each day with a riskScore (probability of an event) by topics.',
    author='bewongm',
    license='',
)


