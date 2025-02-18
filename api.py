from fastapi import FastAPI, HTTPException
import psycopg2
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# Модель для новости
class NewsItem(BaseModel):
    id: int
    title: str
    description: Optional[str] = None
    link: str
    image_url: Optional[str] = None
    published_at: str
    category: Optional[str] = None  # Разрешаем значение None

# Инициализация FastAPI
app = FastAPI()

# Функция для подключения к базе данных
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="centerbeam.proxy.rlwy.net",
            port=25664,
            dbname="railway",
            user="postgres",
            password="vIaIOkXRDMfPjWjDfFxqRsZoqzQezXtO"
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка подключения к базе данных: {e}")

# Эндпоинт для получения всех новостей
@app.get("/news", response_model=List[NewsItem])
def get_all_news():
    conn = connect_to_db()
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, title, description, link, image_url, published_at, category FROM news")
        news = cursor.fetchall()
        return [
            {
                "id": item[0],
                "title": item[1],
                "description": item[2],
                "link": item[3],
                "image_url": item[4],
                "published_at": item[5].isoformat(),
                "category": item[6] if item[6] else None  # Если category равно NULL, возвращаем None
            }
            for item in news
        ]

# Эндпоинт для получения новостей по категории
@app.get("/news/category/{category}", response_model=List[NewsItem])
def get_news_by_category(category: str):
    conn = connect_to_db()
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id, title, description, link, image_url, published_at, category FROM news WHERE category = %s",
            (category,)
        )
        news = cursor.fetchall()
        return [
            {
                "id": item[0],
                "title": item[1],
                "description": item[2],
                "link": item[3],
                "image_url": item[4],
                "published_at": item[5].isoformat(),
                "category": item[6] if item[6] else None  # Если category равно NULL, возвращаем None
            }
            for item in news
        ]

# Эндпоинт для получения одной новости по ID
@app.get("/news/{news_id}", response_model=NewsItem)
def get_news_by_id(news_id: int):
    conn = connect_to_db()
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id, title, description, link, image_url, published_at, category FROM news WHERE id = %s",
            (news_id,)
        )
        news = cursor.fetchone()
        if not news:
            raise HTTPException(status_code=404, detail="Новость не найдена")
        return {
            "id": news[0],
            "title": news[1],
            "description": news[2],
            "link": news[3],
            "image_url": news[4],
            "published_at": news[5].isoformat(),
            "category": news[6] if news[6] else None  # Если category равно NULL, возвращаем None
        }

# Эндпоинт для поиска новостей по ключевому слову
@app.get("/news/search/{keyword}", response_model=List[NewsItem])
def search_news(keyword: str):
    conn = connect_to_db()
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT id, title, description, link, image_url, published_at, category FROM news WHERE title ILIKE %s OR description ILIKE %s",
            (f"%{keyword}%", f"%{keyword}%")
        )
        news = cursor.fetchall()
        return [
            {
                "id": item[0],
                "title": item[1],
                "description": item[2],
                "link": item[3],
                "image_url": item[4],
                "published_at": item[5].isoformat(),
                "category": item[6] if item[6] else None  # Если category равно NULL, возвращаем None
            }
            for item in news
        ]
