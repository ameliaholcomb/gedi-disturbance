from setuptools import find_packages, setup

setup(
    name="src",
    version="0.0.1",
    author="Amelia Holcomb",
    author_email="ah2174@cl.cam.ac.uk",
    description="This project uses GEDI space-borne LiDAR to measure the biomass losses and changes to forest canopy structure caused by small-scale tropical disturbances.",
    url="url-to-github-page",
    packages=find_packages(),
    test_suite="src.tests.test_all.suite",
)
