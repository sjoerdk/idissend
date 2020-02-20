=========
IDIS send
=========

.. image:: https://github.com/sjoerdk/idissend/workflows/build/badge.svg
        :target: https://github.com/sjoerdk/idissend/actions?query=workflow%3Abuild
        :alt: Build Status

.. image:: https://img.shields.io/pypi/v/idissend.svg
        :target: https://pypi.python.org/pypi/idissend

.. image:: https://readthedocs.org/projects/idissend/badge/?version=latest
        :target: https://idissend.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/sjoerdk/idissend/shield.svg
     :target: https://pyup.io/repos/github/sjoerdk/idissend/
     :alt: Updates



Automate sending incoming files to IDIS anonymization server


* Free software: GNU General Public License v3
* Documentation: https://idissend.readthedocs.io.


Features
--------

* Manages files from incoming dir to pending, to completed
* Groups files per study and makes an informed guess about whether a study is complete.
* Creates jobs with IDIS anonymization server via web API
* Determines whether anonymization is complete

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
