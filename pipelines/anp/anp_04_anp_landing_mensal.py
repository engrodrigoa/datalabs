"""ANP mensal CSVs -> bronze.anp_landing_mensal. Logic shared in anp_landing.py."""
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pipelines.anp.anp_landing import run_landing

if __name__ == "__main__":
    sys.exit(run_landing("mensal"))
