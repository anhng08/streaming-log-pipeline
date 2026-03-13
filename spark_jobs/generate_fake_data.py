"""
spark_jobs/generate_fake_data.py
---------------------------------
Sinh fake log data cùng schema với data thật.
Mục đích: demo "real-time processing" cho CV — chạy generator song song với consumer.

Cách chạy:
    uv run python spark_jobs/generate_fake_data.py --mode batch  # sinh 100k rows
    uv run python spark_jobs/generate_fake_data.py --mode stream # giả lập streaming (1 record/giây)
"""

import ast
import os
import random
import time
import argparse
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("vi_VN")
random.seed(42)

# ─────────────────────────────────────────────
# Constants — giữ realistic với data FPT Play
# ─────────────────────────────────────────────
APP_NAMES    = ["VOD", "KPLUS", "FIMS", "CHANNEL", "SPORT"]
EVENTS       = ["PlayVOD", "StopVOD", "PauseVOD", "ResumeVOD", "SeekVOD", "ExitVOD"]
SCREENS      = ["PlayingFromGeneral", "PlayingFromSearch", "PlayingFromRecommend", "PlayingFromCategory"]
FIRMWARES    = ["2.4.14", "2.4.15", "2.5.0", "2.5.1", "3.0.0"]
COUNTRIES    = ["Việt Nam", "Hàn Quốc", "Mỹ", "Nhật Bản", "Trung Quốc", "Thái Lan"]

# Pool of fake MAC addresses (simulate ~5000 users)
MAC_POOL = [
    "".join(random.choices("0123456789ABCDEF", k=12))
    for _ in range(5_000)
]

ITEM_POOL = [str(random.randint(10_000_000, 99_999_999)) for _ in range(500)]
ITEM_NAMES = [fake.sentence(nb_words=4) for _ in range(500)]
ITEM_MAP = dict(zip(ITEM_POOL, ITEM_NAMES))

START_DATE = datetime(2016, 1, 1)
END_DATE   = datetime(2016, 6, 30)


def random_timestamp(start=START_DATE, end=END_DATE) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def make_session_id(mac: str, ts: datetime) -> str:
    return f"{mac}:{ts.strftime('%Y:%m:%d:%H:%M:%S')}:{random.randint(100, 999)}"


def generate_one_record(churned: bool = False) -> dict:
    """
    Sinh 1 record log.
    churned=True → RealTimePlaying thấp, ít session, event chủ yếu là Exit
    """
    mac = random.choice(MAC_POOL)
    ts  = random_timestamp(
        end=END_DATE - timedelta(days=35) if churned else END_DATE
    )

    item_id   = random.choice(ITEM_POOL)
    item_name = ITEM_MAP[item_id]
    duration  = random.randint(600, 7200)   # 10 phút → 2 giờ

    if churned:
        real_time = random.uniform(10, duration * 0.2)     # chỉ xem 20%
        event     = random.choice(["ExitVOD", "StopVOD"])
    else:
        real_time = random.uniform(duration * 0.4, duration)
        event     = random.choice(EVENTS)

    session_ts   = make_session_id(mac, ts)
    subsession   = make_session_id(mac, ts + timedelta(seconds=random.randint(5, 30)))

    return {
        "Mac":                mac,
        "CustomerID":         str(random.randint(100_000, 999_999)),
        "Contract":           f"SGFD{random.randint(10000, 99999)}",
        "Session":            session_ts,
        "SessionMainMenu":    session_ts,
        "SessionSubMenu":     subsession,
        "AppName":            random.choice(APP_NAMES),
        "AppId":              random.choice(APP_NAMES),
        "Event":              event,
        "ItemId":             item_id,
        "ItemName":           item_name,
        "RealTimePlaying":    round(real_time, 1),
        "ElapsedTimePlaying": int(real_time * 1.05),
        "Duration":           duration,
        "Screen":             random.choice(SCREENS),
        "Url":                f"http://fptstream.vn/stream/{mac}/{item_id}.mp4",
        "Ip":                 fake.ipv4_private(),
        "ip_wan":             fake.ipv4_public(),
        "Firmware":           random.choice(FIRMWARES),
        "Folder":             str(random.randint(1, 50)),
        "SubMenuId":          str(random.randint(1, 30)),
        "ChapterId":          str(random.randint(100_000, 999_999)),
        "PublishCountry":     random.choice(COUNTRIES),
        "Directors":          fake.name(),
        "BoxTime":            make_session_id(mac, ts + timedelta(minutes=random.randint(5, 120))),
        "LocalType":          str(random.choice([0, 1])),
        "ListOnFolder":       f"{random.randint(1, 20)},{random.randint(20, 40)}",
        "DefaultGetway":      "192.168.1.1",
        "PrimaryDNS":         "192.168.1.1",
        "SecondaryDNS":       "0.0.0.0",
        "SubnetMask":         "255.255.255.0",
        "LogId":              str(random.randint(1, 100)),
        "Contract":           f"SGFD{random.randint(10000, 99999)}",
    }


def generate_batch(n: int, output_path: str, churn_ratio: float = 0.3):
    """
    Sinh n records, ghi ra file .txt cùng format với data gốc (1 dict per line).
    churn_ratio: tỷ lệ records từ churned users
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print(f"[generate] Đang sinh {n:,} records → {output_path}")
    t0 = time.time()

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(n):
            churned = random.random() < churn_ratio
            record  = generate_one_record(churned=churned)
            f.write(str(record) + "\n")

            if (i + 1) % 10_000 == 0:
                elapsed = time.time() - t0
                print(f"  {i+1:,}/{n:,} records — {elapsed:.1f}s")

    print(f"[generate] ✅ Xong! {n:,} records trong {time.time()-t0:.1f}s")


def simulate_stream(output_dir: str, records_per_second: int = 5):
    """
    Giả lập streaming: ghi liên tục vào file mới mỗi giây.
    Bạn có thể đọc folder này bằng Spark Structured Streaming.
    
    Dừng bằng Ctrl+C.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"[stream] Bắt đầu simulate streaming tới {output_dir}/")
    print(f"[stream] {records_per_second} records/giây — Ctrl+C để dừng")

    batch_num = 0
    try:
        while True:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = os.path.join(output_dir, f"stream_{ts}_{batch_num:04d}.txt")

            with open(path, "w", encoding="utf-8") as f:
                for _ in range(records_per_second):
                    f.write(str(generate_one_record()) + "\n")

            print(f"[stream] Wrote {records_per_second} records → {os.path.basename(path)}")
            batch_num += 1
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[stream] Dừng. Đã sinh {batch_num * records_per_second:,} records total.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    parser.add_argument("--n",    type=int, default=100_000, help="Số records (batch mode)")
    args = parser.parse_args()

    if args.mode == "batch":
        generate_batch(
            n=args.n,
            output_path="data/raw/logt_fake_generated.txt",
            churn_ratio=0.3,
        )
    else:
        simulate_stream(
            output_dir="data/stream_input/",
            records_per_second=5,
        )
