import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pgborg",
    version="0.0.10",
    author="Alexander Boström",
    author_email="abo@kth.se",
    description="Services and utilities for continous archiving and regular full backups of PostgreSQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gita.sys.kth.se/abo/pgborg",
    packages=['pgborg', 'pgborg._private'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    scripts=['bin/pgcad', 'bin/pgbuptool', 'bin/pgcarestore', 'bin/pgdumpd', 'bin/pgdumprestore'],
    # --system-site-packages does not work install_requires=["systemd"],
)

