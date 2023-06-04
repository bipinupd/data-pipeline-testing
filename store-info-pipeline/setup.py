import setuptools

setuptools.setup(
    name="store-info-app",
    version=0.1,
    packages=setuptools.find_packages(),
    install_requires=[ 'apache-beam[gcp]'])