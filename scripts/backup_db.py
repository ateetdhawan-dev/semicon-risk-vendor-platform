import sqlite3, os, datetime, shutil
os.makedirs("backups", exist_ok=True)
src = "data/news.db"
ts = datetime.datetime.utcnow().strftime("%Y-%m-%d")
dst = f"backups/news-{ts}.db"
# atomic copy
con_src = sqlite3.connect(src)
con_dst = sqlite3.connect(dst)
with con_dst:
    con_src.backup(con_dst)
con_dst.close(); con_src.close()
print(f"[OK] Backup -> {dst}")
