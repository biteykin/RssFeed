import feedparser
import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime
import json
from posthog import Posthog  # Импортируем PostHog

# Инициализация PostHog
posthog = Posthog(
    project_api_key='phc_RU6xzmlLZZW1aqv3zobjBN7d9yZA3wdktSqqR0KrzIY',  # Ваш API Key
    host='https://eu.i.posthog.com'  # Укажите хост PostHog
)

def load_config():
    """Загружает конфигурацию из файла config.json."""
    with open('config.json', 'r') as f:
        return json.load(f)

def connect_to_db(config):
    """Подключается к базе данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=config['database']['host'],
            port=config['database']['port'],
            dbname=config['database']['dbname'],
            user=config['database']['user'],
            password=config['database']['password']
        )
        print("Успешное подключение к базе данных!")
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def clean_data(news_item):
    """Очищает данные от лишних символов."""
    news_item['link'] = news_item['link'].replace(';', '')
    news_item['image'] = news_item['image'].replace(';', '') if news_item['image'] else None
    return news_item

def get_rss_feed(url):
    """Собирает новости из RSS-ленты."""
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

def save_news_to_db(conn, news, category):
    """Сохраняет новости в базу данных."""
    with conn.cursor() as cursor:
        for item in news:
            item = clean_data(item)  # Очищаем данные
            try:
                cursor.execute("""
                    INSERT INTO news (title, description, link, image_url, published_at, category)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING;
                """, (item['title'], item['description'], item['link'], item['image'], item['published_at'], category))
                print(f"Новость '{item['title']}' успешно сохранена в базу данных.")

                # Отправка события в PostHog
                posthog.capture(
                    distinct_id='rss-collector',  # Уникальный идентификатор сервиса
                    event='news_saved',  # Название события
                    properties={
                        'title': item['title'],
                        'source': item['link'],
                        'category': category,
                        "$process_person_profile": False  # Отключаем обработку профиля
                    }
                )
            except Exception as e:
                print(f"Ошибка при сохранении новости {item['link']}: {e}")

                # Отправка события об ошибке в PostHog
                posthog.capture(
                    distinct_id='rss-collector',
                    event='news_save_failed',
                    properties={
                        'error': str(e),
                        'link': item['link'],
                        "$process_person_profile": False  # Отключаем обработку профиля
                    }
                )
        conn.commit()

def main():
    """Основная функция."""
    config = load_config()
    conn = connect_to_db(config)

    if not conn:
        print("Не удалось подключиться к базе данных. Выход.")
        return

    all_news = []
    for feed in config['rss_feeds']:
        feed_url = feed['url']
        category = feed['category']
        print(f"Сбор новостей с {feed_url} (тематика: {category})...")
        news = get_rss_feed(feed_url)
        all_news.extend(news)

        # Отправка события о сборе новостей в PostHog
        posthog.capture(
            distinct_id='rss-collector',
            event='news_collected',
            properties={
                'source': feed_url,
                'category': category,
                'count': len(news),
                "$process_person_profile": False  # Отключаем обработку профиля
            }
        )

        # Сохраняем новости в базу данных
        save_news_to_db(conn, news, category)

    print(f"Собрано и сохранено {len(all_news)} новостей.")

    conn.close()

if __name__ == "__main__":
    main()
