import argparse
import time


def run_step(name: str, fn):
    print(f"\n{'='*55}")
    print(f"  STEP: {name.upper()}")
    print(f"{'='*55}")
    t0 = time.time()
    fn()
    elapsed = time.time() - t0
    print(f"\n {name} done in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="FPT Play Log Pipeline")
    parser.add_argument(
        "--step",
        choices=["ingest", "transform", "load", "all"],
        default="all",
        help="Steps (default: all)"
    )
    args = parser.parse_args()

    if args.step in ("ingest", "all"):
        from spark_jobs.ingest import main as ingest_main
        run_step("ingest", ingest_main)

    if args.step in ("transform", "all"):
        from spark_jobs.transform import main as transform_main
        run_step("transform", transform_main)

    if args.step in ("load", "all"):
        from spark_jobs.load import main as load_main
        run_step("load", load_main)

    print("PIPELINE DONE")

if __name__ == "__main__":
    main()
