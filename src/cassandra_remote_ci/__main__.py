# SPDX-License-Identifier: Apache-2.0
"""Entry point for python -m cassandra_remote_ci."""

import sys

from cassandra_remote_ci.local.cli import main

sys.exit(main())
