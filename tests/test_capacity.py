import math
import pytest
from scripts.capacity import estimate


def test_workload_units_do_not_conflate_sessions_and_packets():
    r = estimate(1_000_000, 180, 3, 100, .6)
    assert r['average_active_sessions'] == pytest.approx(2083.3333333)
    assert r['peak_active_sessions'] == pytest.approx(6250)
    assert r['gateway_replicas_before_failure_reserve'] == 105
    assert r['peak_duplex_packets_per_second_at_20ms'] == pytest.approx(625000)
    assert r['redis_script_calls_per_second_for_renewals_only'] == pytest.approx(4166.6666667)


@pytest.mark.parametrize('value', [0, -1, math.inf, math.nan])
def test_invalid_workload_rejected(value):
    with pytest.raises(ValueError):
        estimate(value, 180, 3, 100, .6)
