from setuptools import setup, find_packages

with open('requirements.txt') as f:
    content = f.readlines()
requirements = [x.strip() for x in content]

setup(
    name='RaphaelHealth',
    version='1.0',
    description='Package description',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        # Add any dependencies your package needs here
    ],
)
