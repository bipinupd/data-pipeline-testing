import setuptools

setuptools.setup(
    name="pubsub-to-bq-streaming-store",
    version=0.1,
    packages=setuptools.find_packages(),
    install_requires=[ 'apache-beam[gcp]'])