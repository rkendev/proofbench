"""Pure harness logic: deterministic, offline, and free of any client SDK.

Modules here take data and return data. No broker client, no model client, and no
network client is imported anywhere under the package (INV-P1), which is what keeps
a harness run deterministic and its expected spend at zero.
"""
