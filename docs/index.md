# Welcome to SQLArgon documentation

![CI](https://github.com/performancemedia/sqlargon/workflows/CI/badge.svg)
![Build](https://github.com/performancemedia/sqlargon/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/performancemedia/sqlargon)
![Python](https://img.shields.io/pypi/pyversions/sqlargon)
![Format](https://img.shields.io/pypi/format/sqlargon)
![PyPi](https://img.shields.io/pypi/v/sqlargon)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)

*Wrapper around SQLAlchemy async session, core and native features for Postgres/SQLite*

---
Version: 0.1.0

Docs: [https://performancemedia.github.io/sqlargon/](https://performancemedia.github.io/sqlargon/)

Repository: [https://github.com/performancemedia/sqlargon](https://github.com/performancemedia/sqlargon)


---

## About

This library provides glue code to use sqlalchemy async sessions, core queries and orm models 
from one object which provides somewhat of repository pattern. This solution has few advantages:

- no need to pass `session` object to every function/method. It is stored (and optionally injected) in repository object
- write data access queries in one place
- no need to import `insert`,`update`, `delete`, `select` from sqlalchemy over and over again
- Implicit cast of results to `.scalars().all()` or `.one()`
- Your view model (e.g. FastAPI routes) does not need to know about the underlying storage. Repository class can be replaced at any moment any object providint similar interface.


## Installation

```shell
pip install sqlargon
```
or

```shell
poetry add sqlargon
```
