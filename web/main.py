from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import psycopg2
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Подключение к базе
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "musicdb")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mysecretpassword")

app = FastAPI(title="MusicStats")

# Настройка шаблонов и статических файлов
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

def get_db():
    """Подключение к базе"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Главная страница"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/last")
async def get_last(limit: int = 20):
    """Последние треки"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT track_name, artist_name, played_at 
        FROM listens 
        ORDER BY played_at DESC 
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [
        {"track": row[0], "artist": row[1], "played_at": row[2]}
        for row in rows
    ]

@app.get("/api/top-artists")
async def get_top_artists(limit: int = 10):
    """Топ исполнителей"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT artist_name, COUNT(*) as plays
        FROM listens
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [{"artist": row[0], "plays": row[1]} for row in rows]

@app.get("/api/top-tracks")
async def get_top_tracks(limit: int = 10):
    """Топ треков"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT track_name, artist_name, COUNT(*) as plays
        FROM listens
        GROUP BY track_name, artist_name
        ORDER BY plays DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [{"track": row[0], "artist": row[1], "plays": row[2]} for row in rows]

@app.get("/api/stats/daily")
async def get_daily_stats(days: int = 7):
    """Статистика по дням"""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            DATE(played_at) as date,
            COUNT(*) as plays
        FROM listens
        WHERE played_at >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY DATE(played_at)
        ORDER BY date DESC
    """, (days,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [{"date": str(row[0]), "plays": row[1]} for row in rows]

@app.get("/api/stats/total")
async def get_total_stats():
    """Общая статистика"""
    conn = get_db()
    cur = conn.cursor()
    
    # Всего треков
    cur.execute("SELECT COUNT(*) FROM listens")
    total_tracks = cur.fetchone()[0]
    
    # Всего исполнителей
    cur.execute("SELECT COUNT(DISTINCT artist_name) FROM listens")
    total_artists = cur.fetchone()[0]
    
    # Первый трек
    cur.execute("SELECT MIN(played_at) FROM listens")
    first_track = cur.fetchone()[0]
    
    # Последний трек
    cur.execute("SELECT MAX(played_at) FROM listens")
    last_track = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return {
        "total_tracks": total_tracks,
        "total_artists": total_artists,
        "first_track": first_track,
        "last_track": last_track
    }

@app.get("/api/history/{date}")
async def get_history_by_date(date: str):
    """
    Получить историю прослушиваний за конкретную дату
    Формат даты: YYYY-MM-DD (например, 2026-03-11)
    """
    conn = get_db()
    cur = conn.cursor()
    
    # Ищем треки за указанную дату
    cur.execute("""
        SELECT track_name, artist_name, album_name, played_at
        FROM listens
        WHERE DATE(played_at) = %s
        ORDER BY played_at DESC
    """, (date,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [
        {
            "track": row[0],
            "artist": row[1],
            "album": row[2],
            "played_at": row[3]
        }
        for row in rows
    ]

@app.get("/api/available-dates")
async def get_available_dates():
    """Получить список дат, за которые есть данные"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT DISTINCT DATE(played_at) as date
        FROM listens
        ORDER BY date DESC
    """)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [str(row[0]) for row in rows]

@app.get("/api/day-stats/{date}")
async def get_day_stats(date: str):
    """
    Получить полную статистику за конкретный день
    """
    conn = get_db()
    cur = conn.cursor()
    
    # Общая статистика за день
    cur.execute("""
        SELECT 
            COUNT(*) as total_tracks,
            COUNT(DISTINCT artist_name) as unique_artists,
            EXTRACT(HOUR FROM MIN(played_at)) as first_hour,
            EXTRACT(HOUR FROM MAX(played_at)) as last_hour,
            COUNT(DISTINCT EXTRACT(HOUR FROM played_at)) as active_hours
        FROM listens
        WHERE DATE(played_at) = %s
    """, (date,))
    
    total_stats = cur.fetchone()
    
    # Топ треков за день
    cur.execute("""
        SELECT track_name, artist_name, COUNT(*) as plays
        FROM listens
        WHERE DATE(played_at) = %s
        GROUP BY track_name, artist_name
        ORDER BY plays DESC, MAX(played_at) DESC
        LIMIT 5
    """, (date,))
    
    top_tracks = cur.fetchall()
    
    # Топ исполнителей за день
    cur.execute("""
        SELECT artist_name, COUNT(*) as plays
        FROM listens
        WHERE DATE(played_at) = %s
        GROUP BY artist_name
        ORDER BY plays DESC, MAX(played_at) DESC
        LIMIT 5
    """, (date,))
    
    top_artists = cur.fetchall()
    
    # Последние 5 треков за день
    cur.execute("""
        SELECT track_name, artist_name, played_at
        FROM listens
        WHERE DATE(played_at) = %s
        ORDER BY played_at DESC
        LIMIT 5
    """, (date,))
    
    recent_tracks = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return {
        "date": date,
        "total_tracks": total_stats[0],
        "unique_artists": total_stats[1],
        "first_hour": total_stats[2],
        "last_hour": total_stats[3],
        "active_hours": total_stats[4],
        "top_tracks": [
            {"track": t[0], "artist": t[1], "plays": t[2]}
            for t in top_tracks
        ],
        "top_artists": [
            {"artist": a[0], "plays": a[1]}
            for a in top_artists
        ],
        "recent_tracks": [
            {"track": r[0], "artist": r[1], "played_at": r[2]}
            for r in recent_tracks
        ]
    }