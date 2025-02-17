import feedparser
import requests
from bs4 import BeautifulSoup
import psycopg2
import json
from datetime import datetime

# Чтение конфигурационного файла
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

# Подключение к базе данных
def connect_to_db(config):
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        dbname=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password']
    )

# Извлечение новостей из RSS-ленты
def get_rss_feed(url):
    feed = feedparser.parse(url)
    news_items = []

    for entry in feed.entries:
        title = entry.get('title', '')
        description = entry.get('summary', '')
        link = entry.get('link', '')
        published = entry.get('published', '')

        # Извлечение изображения
        image = None
        if 'media_content' in entry:
            image = entry.media_content[0]['url']
        elif 'enclosures' in entry:
            for enclosure in entry.enclosures:
                if enclosure.type.startswith('image'):
                    image = enclosure.href
                    break

        # Если изображение не найдено, попробуем извлечь его из HTML-страницы
        if not image and link:
            try:
                response = requests.get(link)
                soup = BeautifulSoup(response.text, 'html.parser')
                img_tag = soup.find('meta', property='og:image') or soup.find('meta', attrs={'name': 'og:image'})
                if img_tag:
                    image = img_tag['content']
            except Exception as e:
                print(f"Ошибка при загрузке страницы {link}: {e}")

        # Преобразуем дату публикации в формат для PostgreSQL
        published_at = None
        if published:
            try:
                published_at = datetime.strptime(published, '%a, %d %b %Y %H:%M:%S %z')
            except ValueError:
                published_at = datetime.now()

        news_items.append({
            'title': title,
            'description': description,
            'link': link,
            'image': image,
            'published_at': published_at
        })

    return news_items

# Сохранение новости в базу данных
def save_news_to_db(conn, news):
    with conn.cursor() as cursor:
        for item in news:
            try:
                cursor.execute("""
                    INSERT INTO news (title, description, link, image_url, published_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING;
                """, (item['title'], item['description'], item['link'], item['image'], item['published_at']))
            except Exception as e:
                print(f"Ошибка при сохранении новости {item['link']}: {e}")
        conn.commit()

# Основная функция
def main():
    config = load_config()
    conn = connect_to_db(config)

    all_news = []
    for feed_url in config['rss_feeds']:
        print(f"Сбор новостей с {feed_url}...")
        news = get_rss_feed(feed_url)
        all_news.extend(news)

    save_news_to_db(conn, all_news)
    print(f"Собрано и сохранено {len(all_news)} новостей.")

    conn.close()

if __name__ == "__main__":
    main()
