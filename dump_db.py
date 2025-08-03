import sqlite3
import sys

src = "cardio.db"
dump = "dump_all.sql"

try:
    conn = sqlite3.connect(src)
    with open(dump, "w", encoding="utf-8") as f:
        for line in conn.iterdump():
            f.write(f"{line}\n")
    print(f"✔ Дамп успешно сохранён в {dump}")
except sqlite3.DatabaseError as e:
    print("❌ Не удалось сделать дамп:", e, file=sys.stderr)
    sys.exit(1)
finally:
    conn.close()