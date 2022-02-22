import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("../../VERSION", "r", encoding="utf-8") as fh:
    version = fh.readline().strip()

setuptools.setup(
    name="libsampled",
    version=version,
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
    python_requires=">=3.6",
)
