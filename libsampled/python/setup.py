from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="libsampled",
    version="0.1.5",
    author="Simon Mirco",
    author_email="python@box.id.au",
    description="Collect samples/events for ingest to sampled",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/boxidau/sampled",
    project_urls={
        "Bug Tracker": "https://github.com/boxidau/sampled/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"libsampled": "libsampled"},
    packages=find_packages(exclude=("tests")),
    python_requires=">=3.6",
)
