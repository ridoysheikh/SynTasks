from datetime import datetime

import pytest

from syntasks.scheduler.cron import CronExpression, is_now


@pytest.mark.parametrize(
    ("expression", "moment", "expected"),
    [
        ("* * * * *", datetime(2024, 5, 17, 10, 33), True),
        ("33 10 * * *", datetime(2024, 5, 17, 10, 33), True),
        ("34 10 * * *", datetime(2024, 5, 17, 10, 33), False),
        ("*/15 * * * *", datetime(2024, 5, 17, 10, 30), True),
        ("*/15 * * * *", datetime(2024, 5, 17, 10, 31), False),
        ("5/10 * * * *", datetime(2024, 5, 17, 10, 25), True),
        ("5/10 * * * *", datetime(2024, 5, 17, 10, 20), False),
        ("0-10/5 * * * *", datetime(2024, 5, 17, 10, 10), True),
        ("1,2,3 * * * *", datetime(2024, 5, 17, 10, 2), True),
        ("0 9-17 * * mon-fri", datetime(2024, 5, 17, 12, 0), True),  # Friday
        ("0 9-17 * * mon-fri", datetime(2024, 5, 18, 12, 0), False),  # Saturday
        ("0 0 * * 0", datetime(2024, 5, 19, 0, 0), True),  # Sunday
        ("0 0 * * 7", datetime(2024, 5, 19, 0, 0), True),  # Sunday as 7
        ("0 0 1 jan *", datetime(2024, 1, 1, 0, 0), True),
        ("0 0 1 JAN *", datetime(2024, 2, 1, 0, 0), False),
        # Both day fields restricted: either one matching is enough.
        ("0 0 13 * fri", datetime(2024, 5, 17, 0, 0), True),  # Friday the 17th
        ("0 0 13 * fri", datetime(2024, 5, 13, 0, 0), True),  # Monday the 13th
        ("0 0 13 * fri", datetime(2024, 5, 14, 0, 0), False),
        ("@hourly", datetime(2024, 5, 17, 10, 0), True),
        ("@daily", datetime(2024, 5, 17, 10, 0), False),
        ("@weekly", datetime(2024, 5, 19, 0, 0), True),
    ],
)
def test_is_now(expression: str, moment: datetime, expected: bool) -> None:
    assert is_now(expression, moment) is expected


@pytest.mark.parametrize(
    "expression",
    [
        "",
        "* * * *",
        "* * * * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "5-1 * * * *",
        "a * * * *",
        "1,,2 * * * *",
    ],
)
def test_invalid(expression: str) -> None:
    with pytest.raises(ValueError):
        CronExpression.parse(expression)
