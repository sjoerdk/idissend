=========
IDIS send
=========

.. image:: https://github.com/sjoerdk/idissend/workflows/build/badge.svg
        :target: https://github.com/sjoerdk/idissend/actions?query=workflow%3Abuild
        :alt: Build Status

.. image:: https://codecov.io/gh/sjoerdk/idissend/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/sjoerdk/idissend

.. image:: https://pyup.io/repos/github/sjoerdk/idissend/shield.svg
     :target: https://pyup.io/repos/github/sjoerdk/idissend/
     :alt: Updates

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/ambv/black


Automate sending incoming files to IDIS anonymization server


* Free software: GNU General Public License v3
* Documentation: https://idissend.readthedocs.io.


Features
--------

* Manages files from incoming dir to pending, to completed
* Groups files per study and makes an informed guess about whether a study is complete.
* Creates jobs with IDIS anonymization server via web API
* Determines whether anonymization is complete

Install
-------

    pip install git+ssh://git@github.com:sjoerdk/idissend.git

See examples/pipeline.py for a basic pipeline example

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
