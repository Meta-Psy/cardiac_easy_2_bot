import sqlite3
import sys

new_db = "cardio_repaired.db"
dump = "dump_all.sql"

try:
    conn = sqlite3.connect(new_db)
    sql = open(dump, "r", encoding="utf-8").read()
    conn.executescript(sql)
    print(f"✔ Новая БД успешно создана: {new_db}")
except Exception as e:
    print("❌ Ошибка при восстановлении:", e, file=sys.stderr)
    sys.exit(1)
finally:
    conn.close()