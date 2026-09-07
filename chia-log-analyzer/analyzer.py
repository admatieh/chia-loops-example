from collections import Counter
import re


def analyze_log_text(text: str) -> dict:
    lines = text.splitlines()

    error_count = 0
    warning_count = 0
    info_count = 0

    ip_pattern = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    ips = []

    for line in lines:
        upper_line = line.upper()

        if "ERROR" in upper_line:
            error_count += 1

        if "WARNING" in upper_line:
            warning_count += 1

        if "INFO" in upper_line:
            info_count += 1

        ips.extend(re.findall(ip_pattern, line))

    ip_counts = Counter(ips)

    return {
        "total_lines": len(lines),
        "errors": error_count,
        "warnings": warning_count,
        "info": info_count,
        "ip_addresses": dict(ip_counts),
    }
