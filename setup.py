#!/usr/bin/env python3
import os
import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand
from setuptools.command.sdist import sdist as SdistCommand

try:
    from setuptools_rust import RustExtension
except ImportError:
    import subprocess

    errno = subprocess.call([sys.executable, "-m", "pip", "install", "setuptools-rust"])
    if errno:
        print("Please install setuptools-rust package")
        raise SystemExit(errno)
    else:
        from setuptools_rust import RustExtension


class CargoModifiedSdist(SdistCommand):
    def make_release_tree(self, base_dir, files):
        """Stages the files to be included in archives"""
        super().make_release_tree(base_dir, files)


setup_requires = ["setuptools-rust>=0.10.1", "wheel"]
install_requires = []

setup(
    name="pyd4",
    version="0.1.13",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Rust",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
    packages=["pyd4"],
    rust_extensions=[RustExtension("pyd4.pyd4", "Cargo.toml", debug=False)],
    install_requires=install_requires,
    setup_requires=setup_requires,
    include_package_data=True,
    zip_safe=False,
    cmdclass={"sdist": CargoModifiedSdist},
)
