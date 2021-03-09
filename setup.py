import io

from setuptools import find_packages, setup

with io.open("README.md", "rt", encoding="utf8") as f:
    readme = f.read()

setup(
    name="generic-site",
    version="1.0.0",
    url="",
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=["flask", "pydub", "sqlalchemy", "flask-admin", "flask-security"],
    extras_require={
        "admin": ["matplotlib", "seaborn"],
        "test": ["pytest", "coverage"]
    },
)
