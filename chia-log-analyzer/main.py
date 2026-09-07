from pathlib import Path

from chia.base.ChiaFunction import ChiaFunction, get
from analyzer import analyze_log_text


@ChiaFunction(resources={"analysis": 1})
def analyze_file(path_str: str) -> dict:
    path = Path(path_str)

    text = path.read_text()

    result = analyze_log_text(text)

    return {
        "file": path.name,
        "result": result,
    }


@ChiaFunction(resources={"aggregation": 1})
def aggregate_results(results: list[dict]) -> dict:
    total_files = len(results)
    total_lines = 0
    total_errors = 0
    total_warnings = 0
    total_info = 0

    for item in results:
        data = item["result"]

        total_lines += data["total_lines"]
        total_errors += data["errors"]
        total_warnings += data["warnings"]
        total_info += data["info"]

    return {
        "files_processed": total_files,
        "total_lines": total_lines,
        "errors": total_errors,
        "warnings": total_warnings,
        "info": total_info,
    }


def main():
    log_files = sorted(Path("sample_logs").glob("*.log"))

    print(f"Found {len(log_files)} log files.")

    analysis_refs = []

    for path in log_files:
        ref = analyze_file.chia_remote(str(path))
        analysis_refs.append(ref)

        print(f"Submitted: {path.name}")
        print(f"ObjectRef: {ref}")

    analysis_results = get(analysis_refs)

    print("\nIndividual results:")

    for item in analysis_results:
        print(item)

    aggregate_ref = aggregate_results.chia_remote(analysis_results)

    print("\nAggregation task submitted.")
    print(f"ObjectRef: {aggregate_ref}")

    final_report = get(aggregate_ref)

    print("\nFinal report:")
    print(final_report)


if __name__ == "__main__":
    main()
