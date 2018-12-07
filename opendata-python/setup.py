import setuptools


setuptools.setup(
    name="goldencheetah-opendata",
    version=0.2,
    author="Aart Goossens",
    author_email="aart@goossens.me  ",
    description="Python client library for the GoldenCheetah Open Data project",
    long_description_content_type="text/x-rst",
    url="https://github.com/GoldenCheetah/OpenData",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3",
        "pandas",
        "pkgsettings",
    ],
    tests_require=[
        "pytest",
        "pytest-flake8",
    ],
    setup_requires=[
        "pytest-runner",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
    ],
)
