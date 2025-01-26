from setuptools import setup, find_packages

setup(
    name="threads-insights-sdk",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "python-dotenv",
    ],
    python_requires=">=3.7",
)