from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Price Optimization'
LONG_DESCRIPTION = 'Package for APi consumption'
REQUIRED_PACKAGES = [
    'numpy==1.25.2',
    'pandas==2.1.0',
    'pymongo==4.5.0',
    'scipy==1.11.2',
    'seaborn==0.12.2'
]

setup(
        name="price_optimization", 
        version=VERSION,
        author="Sidney Guaro, Linneaus Bundalian",
        author_email="sguaro@quibblerm.com, lbundalian@quibblerm.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=REQUIRED_PACKAGES,
        keywords=['python', 'price optimization'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ]
)