"""An example of a pipeline receiving data and sending it to IDIS.

streams and users are all hardcoded in this minimal example, but will probably
be in a database for production
"""

import logging

from idissend.pipeline import get_pipeline_instance

# Make sure you see some logging output
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

pipeline = get_pipeline_instance(base_path="/tmp/idissend")
pipeline.assert_all_paths()
pipeline.run_once()
