
def format_time(h_float):
    """Converts 13.98 -> '13:59', 25.5 -> '01:30'."""
    h_float = h_float % 24
    h = int(h_float)
    # Use round to handle floating point imprecision
    m = int(round((h_float - h) * 60))
    if m == 60:
        h += 1
        m = 0
        if h == 24: h = 0
    return f"{h:02d}:{m:02d}"

def normalize_hour(h_float):
    """Wraps 25.0 -> 1.0, -1.0 -> 23.0"""
    return h_float % 24

def classify_zone(z_name):
    """Returns 'CONSOLE' or 'STANDARD'."""
    z = z_name.lower()
    if any(x in z for x in ['ps5', 'playstation', 'auto', 'sim', 'авто', 'сим']):
        return 'CONSOLE'
    return 'STANDARD'

if __name__ == "__main__":
    # Test Cases
    assert format_time(13.98333) == "13:59", f"Got {format_time(13.98333)}"
    assert format_time(25.5) == "01:30", f"Got {format_time(25.5)}"
    assert format_time(8.0) == "08:00", f"Got {format_time(8.0)}"
    # Edge case: slightly less than next hour
    assert format_time(13.9999) == "14:00", f"Got {format_time(13.9999)}"

    assert normalize_hour(25) == 1
    assert normalize_hour(24) == 0

    assert classify_zone("PlayStation 5") == "CONSOLE"
    assert classify_zone("Zelle Bootcamp") == "STANDARD"
    assert classify_zone("Auto Sim") == "CONSOLE"

    print("✅ All tests passed")
