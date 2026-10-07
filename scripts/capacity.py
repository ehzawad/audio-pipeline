"""Transparent planning arithmetic, NOT a measured fleet-capacity benchmark."""
import argparse
import json
import math


def estimate(sessions_per_day, duration_seconds, peak_factor, calls_per_gateway,
             target_utilization, lease_seconds=9):
    values = [sessions_per_day, duration_seconds, peak_factor, calls_per_gateway,
              target_utilization, lease_seconds]
    if any(not math.isfinite(x) or x <= 0 for x in values) or target_utilization > 1:
        raise ValueError("finite positive inputs and utilization <= 1 are required")
    arrivals = sessions_per_day / 86400
    active = arrivals * duration_seconds * peak_factor
    renewals = active / (lease_seconds / 3)
    return {
        "scope": "planning assumptions only; constant duration and peak arrival multiplier",
        "sessions_per_day": sessions_per_day,
        "average_new_sessions_per_second": arrivals,
        "average_active_sessions": arrivals * duration_seconds,
        "peak_active_sessions": active,
        "assumed_calls_per_gateway_at_slo": calls_per_gateway,
        "gateway_replicas_before_failure_reserve": math.ceil(active / (calls_per_gateway * target_utilization)),
        "peak_duplex_packets_per_second_at_20ms": active * 100,
        "peak_lease_renewals_per_second": renewals,
        "redis_script_calls_per_second_for_renewals_only": renewals * 2,
        "raw_pcm_duplex_megabits_per_second": active * (16000 + 24000) * 2 * 8 / 1e6,
        "phone_mulaw_duplex_megabits_per_second_before_base64": active * 8000 * 2 * 8 / 1e6,
        "excluded": ["model compute", "checkpoints", "signaling", "TLS/JSON/base64 overhead",
                     "regional skew", "failure spare capacity", "burst distribution", "provider quotas"]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sessions-per-day", type=float, default=1_000_000)
    p.add_argument("--duration-seconds", type=float, default=180)
    p.add_argument("--peak-factor", type=float, default=3)
    p.add_argument("--calls-per-gateway", type=float, required=True,
                   help="Measured capacity at your SLO, or an explicitly labeled assumption")
    p.add_argument("--target-utilization", type=float, default=.6)
    args = p.parse_args()
    print(json.dumps(estimate(**vars(args)), indent=2))


if __name__ == "__main__":
    main()
