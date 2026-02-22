"""
Precedence rules for harmonization (Option A)

Order matters: first non-null value is used.
"""

PRECEDENCE_RULES = {
    "PUFNEWEMPSTAT": {
        "order": [
            "NEWEMPST",
            "CEMPST2",
            "CEMPST1",
            "ANSOEMPSTAT"
        ]
    }
}
