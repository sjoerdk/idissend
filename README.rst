=========
IDIS send
=========


.. image:: https://img.shields.io/pypi/v/idissend.svg
        :target: https://pypi.python.org/pypi/idissend

.. image:: https://img.shields.io/travis/sjoerdk/idissend.svg
        :target: https://travis-ci.org/sjoerdk/idissend

.. image:: https://readthedocs.org/projects/idis-send/badge/?version=latest
        :target: https://idis-send.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/sjoerdk/idissend/shield.svg
     :target: https://pyup.io/repos/github/sjoerdk/idissend/
     :alt: Updates



Automate sending incoming files to IDIS anonymization server

* Free software: GNU General Public License v3
* Documentation: https://idis-send.readthedocs.io.


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
