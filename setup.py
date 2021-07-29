"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="dataworks-athena-reconciliation-launcher",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A lambda that launches a batch job from glue event notifications",
    long_description="A lambda that launches a batch job from glue event notifications",
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["batch_job_trigger=batch_job_launcher:main"]},
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse", "boto3"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
