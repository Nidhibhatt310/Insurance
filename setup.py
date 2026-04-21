from setuptools import setup, find_packages

setup(
    name="insurance",
    version="0.0.1",
    description="Insurance project",
    author="abc",
    packages=find_packages(where='./src'),
    package_dir={'': './src'},
    install_requires=['setuptools'],
    entry_points={
        'packages':[
            "main=insurance.main:main"
        ]
    }
)