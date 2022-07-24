"""
Modules Distributions.
"""
import os

from setuptools import find_packages, setup

install_requires = ["oss2>=2.14.0"]


def _read(file):
    return (
        open(os.path.join(os.path.dirname(__file__), file), encoding="utf")
        .read()
        .strip()
    )


with open("test_requirements.txt", encoding="utf-8") as f_r:
    tests_requirements = [line.strip() for line in f_r.readlines()]

setup(
    name="aiooss2",
    version="0.1.2",
    description="Async client for aliyun OSS",
    author="Yanxiang Gao",
    author_email="mishanyo1001@gmail.com",
    download_url="https://github.com/karajan1001/aiooss2",
    license="Apache-2.0 License",
    install_requires=["aiohttp>=3.7.4", "oss2>=2.14.0"],
    extras_require={"tests": tests_requirements},
    keywords="oss, aiohttp",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(exclude=["tests"]),
)
