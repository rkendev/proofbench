"""Protocols the harness is written against, so no count is ever taken by eye.

INV-P2: every reported duplication or loss count comes from committed code diffing
the sink ledger against the expected saga ledger. The diff protocol lives in
``ledger``; PB-T1 lands the interface only, with no implementation.
"""
