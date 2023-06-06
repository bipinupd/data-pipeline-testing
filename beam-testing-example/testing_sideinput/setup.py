import setuptools

setuptools.setup(name="testing_side_input",
                 version=0.1,
                 packages=setuptools.find_packages(),
                 install_requires=['apache-beam[gcp]'])
