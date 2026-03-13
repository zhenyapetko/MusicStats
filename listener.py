#!/usr/bin/env python3
"""
MusicStats Listener - скрипт для сбора истории прослушиваний из Spotify
"""

import os
import time
from datetime import datetime
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

# Загружаем переменные из файла .env
load_dotenv()

# ==================== НАСТРОЙКИ ====================
# Spotify API
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

# База данных
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "musicdb")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mysecretpassword")

# Настройки скрипта
CHECK_INTERVAL = 60  # Проверять каждые 60 секунд

# Проверяем, что все необходимые переменные заданы
if not all([SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI]):
    print("❌ ОШИБКА: Не заданы переменные окружения Spotify!")
    print("Убедись, что в файле .env есть SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET и SPOTIFY_REDIRECT_URI")
    exit(1)

# ==================== ПОДКЛЮЧЕНИЕ К SPOTIFY ====================
def get_spotify_client():
    """
    Создает и возвращает клиент Spotify API
    При первом запуске откроет браузер для авторизации
    """
    try:
        # SpotifyOAuth автоматически сохранит токен в файл .cache
        auth_manager = SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            scope="user-read-recently-played",  # Нам нужно только недавно прослушанное
            cache_path=".spotify_cache",  # Файл для хранения токена
            show_dialog=True  # Показывать диалог при первом входе
        )
        return spotipy.Spotify(auth_manager=auth_manager)
    except Exception as e:
        print(f"❌ Ошибка подключения к Spotify: {e}")
        return None

# ==================== ПОДКЛЮЧЕНИЕ К БАЗЕ ====================
def get_db_connection():
    """Создает и возвращает подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        return None

# ==================== СОХРАНЕНИЕ ТРЕКА ====================
def save_track(conn, track_data):
    """
    Сохраняет один трек в базу данных.
    Использует ON CONFLICT, чтобы не дублировать записи.
    Возвращает True, если трек новый, False если уже был.
    """
    try:
        cursor = conn.cursor()
        
        # Извлекаем данные из ответа Spotify
        track = track_data['track']
        track_name = track['name']
        # Берем первого исполнителя (чаще всего он главный)
        artist_name = track['artists'][0]['name']
        album_name = track['album']['name']
        # Spotify отдает время в формате ISO 8601, например "2024-01-01T12:34:56.789Z"
        played_at_str = track_data['played_at']
        
        # Преобразуем строку в datetime объект
        # Заменяем Z на +00:00 для совместимости
        played_at = datetime.fromisoformat(played_at_str.replace('Z', '+00:00'))
        
        # SQL запрос с защитой от дубликатов
        sql = """
        INSERT INTO listens (track_name, artist_name, album_name, played_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (played_at) DO NOTHING
        RETURNING id;
        """
        
        cursor.execute(sql, (track_name, artist_name, album_name, played_at))
        conn.commit()
        
        # Проверяем, была ли вставлена новая запись
        if cursor.rowcount > 0:
            # Получаем id вставленной записи
            new_id = cursor.fetchone()[0]
            print(f"  ➕ Новый трек: {track_name} - {artist_name}")
            return True
        else:
            print(f"  ⏩ Уже есть: {track_name} - {artist_name}")
            return False
            
    except Exception as e:
        print(f"  ❌ Ошибка при сохранении трека: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

# ==================== ОСНОВНОЙ ЦИКЛ ====================
def main():
    """Главная функция программы"""
    print("=" * 50)
    print("🎵 MusicStats Listener запущен")
    print(f"📅 Время старта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Интервал проверки: {CHECK_INTERVAL} секунд")
    print("=" * 50)
    
    # Подключаемся к Spotify
    sp = get_spotify_client()
    if not sp:
        print("❌ Не удалось подключиться к Spotify. Выход.")
        return
    
    # Основной бесконечный цикл
    while True:
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n🔄 [{current_time}] Проверяем новые треки...")
            
            # Получаем последние 50 прослушанных треков
            # Это максимальное количество, которое отдает Spotify
            results = sp.current_user_recently_played(limit=50)
            
            if results and results['items']:
                tracks_count = len(results['items'])
                print(f"📊 Найдено треков в истории: {tracks_count}")
                
                # Подключаемся к базе
                conn = get_db_connection()
                if not conn:
                    print("❌ Нет соединения с БД. Ждем...")
                    time.sleep(10)
                    continue
                
                # Считаем новые треки
                new_tracks = 0
                for item in results['items']:
                    if save_track(conn, item):
                        new_tracks += 1
                
                conn.close()
                
                print(f"✅ Итого: {new_tracks} новых треков сохранено")
                
                # Если новых треков много, можно показать общее количество
                if new_tracks > 0:
                    # Подключаемся еще раз чтобы посчитать общее количество
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM listens")
                        total = cursor.fetchone()[0]
                        cursor.close()
                        conn.close()
                        print(f"📚 Всего в базе: {total} треков")
            else:
                print("😴 Нет новых треков")
                
        except Exception as e:
            print(f"💥 Критическая ошибка: {e}")
            print("   Ждем 10 секунд и пробуем снова...")
            time.sleep(10)
            continue
        
        # Ждем перед следующей проверкой
        print(f"💤 Спим {CHECK_INTERVAL} секунд...")
        time.sleep(CHECK_INTERVAL)

# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Программа остановлена пользователем")
        print("До встречи!")