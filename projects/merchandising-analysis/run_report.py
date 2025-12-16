import subprocess
import sys
from datetime import datetime

SCRIPTS = [
    ("extract.py", "projects/merchandising-analysis/extract.py"),
    ("build_variant_sales.py", "projects/merchandising-analysis/scripts/build_variant_sales.py"),
    ("build_listing_sales.py", "projects/merchandising-analysis/scripts/build_listing_sales.py"),
    ("build_summary_stats.py", "projects/merchandising-analysis/scripts/build_summary_stats.py"),
    ("build_collection_analysis.py", "projects/merchandising-analysis/scripts/build_collection_analysis.py"),
    ("build_report.py", "projects/merchandising-analysis/scripts/build_report.py"),
]


def run():
    print("=" * 60)
    print(f"MERCHANDISING ANALYSIS PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    for name, path in SCRIPTS:
        print(f"\n{'─' * 60}")
        print(f"▶ Running {name}...")
        print(f"{'─' * 60}\n")
        
        result = subprocess.run([sys.executable, path], cwd=".")
        
        if result.returncode != 0:
            print(f"\n✗ {name} failed with exit code {result.returncode}")
            sys.exit(1)
    
    print(f"\n{'=' * 60}")
    print(f"✓ PIPELINE COMPLETE")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Report: projects/merchandising-analysis/merchandising_report.html")
    print("=" * 60)


if __name__ == "__main__":
    run()