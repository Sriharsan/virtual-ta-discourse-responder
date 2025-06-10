#!/usr/bin/env python3
"""
Enhanced Discourse Scraper for TDS Virtual TA
Scrapes Discourse forum posts within a date range and stores them in the knowledge base.
"""

import argparse
import requests
import json
import sqlite3
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
import os
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiscourseScraperEnhanced:
    def __init__(self, base_url, api_key=None, username=None, db_path="tds_knowledge.db"):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.username = username
        self.db_path = db_path
        self.session = requests.Session()
        
        # Set up authentication if provided
        if api_key and username:
            self.session.headers.update({
                'Api-Key': api_key,
                'Api-Username': username
            })
        
        self.setup_database()
    
    def setup_database(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discourse_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                url TEXT UNIQUE,
                category TEXT,
                created_at TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                topic_id INTEGER,
                post_number INTEGER,
                username TEXT,
                likes_count INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path}")
    
    def get_categories(self):
        """Get list of available categories"""
        try:
            response = self.session.get(f"{self.base_url}/categories.json")
            response.raise_for_status()
            data = response.json()
            
            categories = []
            for category in data.get('category_list', {}).get('categories', []):
                categories.append({
                    'id': category['id'],
                    'name': category['name'],
                    'slug': category['slug']
                })
            
            return categories
        
        except Exception as e:
            logger.error(f"Error fetching categories: {e}")
            return []
    
    def find_category_by_name(self, category_name):
        """Find category ID by name"""
        categories = self.get_categories()
        for category in categories:
            if category_name.lower() in category['name'].lower():
                return category['id']
        return None
    
    def get_topics_from_category(self, category_id, start_date=None, end_date=None):
        """Get topics from a specific category within date range"""
        try:
            url = f"{self.base_url}/c/{category_id}.json"
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            
            topics = []
            for topic in data.get('topic_list', {}).get('topics', []):
                topic_date = datetime.fromisoformat(topic['created_at'].replace('Z', '+00:00'))
                
                # Filter by date range if provided
                if start_date and topic_date < start_date:
                    continue
                if end_date and topic_date > end_date:
                    continue
                
                topics.append({
                    'id': topic['id'],
                    'title': topic['title'],
                    'created_at': topic['created_at'],
                    'posts_count': topic['posts_count']
                })
            
            logger.info(f"Found {len(topics)} topics in category {category_id}")
            return topics
        
        except Exception as e:
            logger.error(f"Error fetching topics from category {category_id}: {e}")
            return []
    
    def scrape_topic(self, topic_id):
        """Scrape all posts from a topic"""
        try:
            url = f"{self.base_url}/t/{topic_id}.json"
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            
            topic_title = data.get('title', '')
            posts = data.get('post_stream', {}).get('posts', [])
            
            scraped_posts = []
            for post in posts:
                post_data = self.extract_post_data(post, topic_title, topic_id)
                if post_data:
                    scraped_posts.append(post_data)
            
            logger.info(f"Scraped {len(scraped_posts)} posts from topic {topic_id}")
            return scraped_posts
        
        except Exception as e:
            logger.error(f"Error scraping topic {topic_id}: {e}")
            return []
    
    def extract_post_data(self, post, topic_title, topic_id):
        """Extract relevant data from a post"""
        try:
            # Clean HTML content
            content = self.clean_html_content(post.get('cooked', ''))
            
            # Skip very short posts
            if len(content.strip()) < 20:
                return None
            
            return {
                'title': topic_title,
                'content': content,
                'url': f"{self.base_url}/t/{topic_id}/{post.get('post_number', 1)}",
                'category': 'TDS',  # Default category
                'created_at': post.get('created_at', ''),
                'topic_id': topic_id,
                'post_number': post.get('post_number', 1),
                'username': post.get('username', ''),
                'likes_count': post.get('actions_summary', [{}])[0].get('count', 0) if post.get('actions_summary') else 0
            }
        
        except Exception as e:
            logger.error(f"Error extracting post data: {e}")
            return None
    
    def clean_html_content(self, html_content):
        """Clean HTML content and extract text"""
        if not html_content:
            return ""
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text and clean up whitespace
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def store_posts(self, posts):
        """Store posts in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stored_count = 0
        for post in posts:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO discourse_posts 
                    (title, content, url, category, created_at, topic_id, post_number, username, likes_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post['title'],
                    post['content'],
                    post['url'],
                    post['category'],
                    post['created_at'],
                    post['topic_id'],
                    post['post_number'],
                    post['username'],
                    post['likes_count']
                ))
                stored_count += 1
            
            except Exception as e:
                logger.error(f"Error storing post: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {stored_count} posts in database")
        return stored_count
    
    def scrape_by_date_range(self, start_date, end_date, categories=None, output_json=None):
        """Main scraping method for date range"""
        logger.info(f"Starting scrape from {start_date} to {end_date}")
        
        # Convert string dates to datetime objects
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)
        
        all_posts = []
        
        # Get categories to scrape
        if categories:
            category_ids = []
            for cat_name in categories:
                cat_id = self.find_category_by_name(cat_name)
                if cat_id:
                    category_ids.append(cat_id)
                    logger.info(f"Found category '{cat_name}' with ID {cat_id}")
                else:
                    logger.warning(f"Category '{cat_name}' not found")
        else:
            # Use default TDS category ID (you may need to adjust this)
            category_ids = [34]  # Assuming 34 is the TDS category ID
        
        # Scrape each category
        for category_id in category_ids:
            topics = self.get_topics_from_category(category_id, start_date, end_date)
            
            for topic in topics[:50]:  # Limit to 50 topics per category to avoid overwhelming
                posts = self.scrape_topic(topic['id'])
                all_posts.extend(posts)
                
                # Rate limiting
                time.sleep(1)
        
        # Store posts in database
        if all_posts:
            self.store_posts(all_posts)
        
        # Save to JSON file if requested
        if output_json:
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(all_posts, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(all_posts)} posts to {output_json}")
        
        logger.info(f"Scraping completed. Total posts: {len(all_posts)}")
        return all_posts

def main():
    parser = argparse.ArgumentParser(description='Enhanced Discourse Scraper for TDS Virtual TA')
    parser.add_argument('--url', required=True, help='Discourse forum base URL')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--api-key', help='Discourse API key (optional)')
    parser.add_argument('--username', help='Discourse username (optional)')
    parser.add_argument('--categories', nargs='+', help='Category names to scrape')
    parser.add_argument('--output-json', help='Output JSON file path')
    parser.add_argument('--db-path', default='tds_knowledge.db', help='Database file path')
    
    args = parser.parse_args()
    
    # Validate dates
    try:
        start_date = datetime.fromisoformat(args.start_date)
        end_date = datetime.fromisoformat(args.end_date)
        
        if start_date >= end_date:
            logger.error("Start date must be before end date")
            sys.exit(1)
    
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        sys.exit(1)
    
    # Initialize scraper
    scraper = DiscourseScraperEnhanced(
        base_url=args.url,
        api_key=args.api_key,
        username=args.username,
        db_path=args.db_path
    )
    
    # Start scraping
    try:
        posts = scraper.scrape_by_date_range(
            start_date=start_date,
            end_date=end_date,
            categories=args.categories,
            output_json=args.output_json
        )
        
        print(f"✅ Successfully scraped {len(posts)} posts")
        print(f"📄 Data stored in: {args.db_path}")
        
        if args.output_json:
            print(f"💾 JSON export saved to: {args.output_json}")
    
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
