from setuptools import setup, find_packages

setup(
    name="vmdk2kvm",
    version="3.1.0",
    packages=find_packages(),
    install_requires=[l.strip() for l in open("requirements.txt", encoding="utf-8") if l.strip() and not l.startswith("#")],
    entry_points={"console_scripts": ["vmdk2kvm=vmdk2kvm.__main__:main"]},
)
