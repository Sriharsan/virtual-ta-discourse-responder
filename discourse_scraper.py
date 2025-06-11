#!/usr/bin/env python3
"""
Enhanced TDS Discourse Scraper
Scrapes posts from Tools in Data Science Discourse forum for specified date range
"""

import argparse
import json
import sqlite3
import requests
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional
import re
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TDSDiscourseScraper:
    def __init__(self, base_url: str, api_key: str = None, username: str = None):
        """
        Initialize the Discourse scraper
        
        Args:
            base_url: Base URL of the Discourse forum
            api_key: Optional API key for authenticated requests
            username: Optional username for authenticated requests
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.username = username
        self.session = requests.Session()
        
        # Set headers for API requests
        if api_key and username:
            self.session.headers.update({
                'Api-Key': api_key,
                'Api-Username': username,
                'User-Agent': 'TDS-Discourse-Scraper/1.0'
            })
        else:
            self.session.headers.update({
                'User-Agent': 'TDS-Discourse-Scraper/1.0'
            })
    
    def get_categories(self) -> Dict[str, int]:
        """Get all categories and their IDs"""
        try:
            response = self.session.get(f"{self.base_url}/categories.json")
            response.raise_for_status()
            data = response.json()
            
            categories = {}
            for category in data.get('category_list', {}).get('categories', []):
                categories[category['name']] = category['id']
            
            logger.info(f"Retrieved {len(categories)} categories")
            return categories
        except Exception as e:
            logger.error(f"Error fetching categories: {e}")
            return {}
    
    def get_topics_for_category(self, category_id: int, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get all topics for a specific category within date range"""
        topics = []
        page = 0
        
        while True:
            try:
                # Use the category endpoint to get topics
                url = f"{self.base_url}/c/{category_id}.json"
                params = {'page': page}
                
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                topic_list = data.get('topic_list', {}).get('topics', [])
                
                if not topic_list:
                    break
                
                # Filter topics by date
                filtered_topics = []
                for topic in topic_list:
                    created_at = datetime.fromisoformat(topic['created_at'].replace('Z', '+00:00'))
                    created_at = created_at.replace(tzinfo=None)  # Remove timezone for comparison
                    
                    if start_date <= created_at <= end_date:
                        filtered_topics.append(topic)
                    elif created_at < start_date:
                        # Topics are usually sorted by date, so we can break early
                        logger.info(f"Reached topics older than start date, stopping pagination")
                        return topics
                
                topics.extend(filtered_topics)
                
                # Check if we've reached the end
                if len(topic_list) < 30:  # Discourse typically returns 30 topics per page
                    break
                
                page += 1
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching topics for category {category_id}, page {page}: {e}")
                break
        
        logger.info(f"Retrieved {len(topics)} topics for category {category_id}")
        return topics
    
    def get_topic_posts(self, topic_id: int) -> Dict:
        """Get all posts for a specific topic"""
        try:
            response = self.session.get(f"{self.base_url}/t/{topic_id}.json")
            response.raise_for_status()
            data = response.json()
            
            # Extract relevant information
            topic_info = {
                'id': topic_id,
                'title': data.get('title', ''),
                'category_id': data.get('category_id'),
                'created_at': data.get('created_at'),
                'posts': []
            }
            
            # Get all posts
            for post in data.get('post_stream', {}).get('posts', []):
                post_data = {
                    'id': post.get('id'),
                    'post_number': post.get('post_number'),
                    'created_at': post.get('created_at'),
                    'updated_at': post.get('updated_at'),
                    'username': post.get('username'),
                    'content': self.clean_html(post.get('cooked', '')),
                    'raw_content': post.get('raw', ''),
                    'reply_count': post.get('reply_count', 0),
                    'like_count': post.get('actions_summary', [{}])[0].get('count', 0) if post.get('actions_summary') else 0
                }
                topic_info['posts'].append(post_data)
            
            time.sleep(0.3)  # Rate limiting
            return topic_info
            
        except Exception as e:
            logger.error(f"Error fetching posts for topic {topic_id}: {e}")
            return None
    
    def clean_html(self, html_content: str) -> str:
        """Clean HTML content to extract plain text"""
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', '', html_content)
        # Decode HTML entities
        clean_text = clean_text.replace('&nbsp;', ' ')
        clean_text = clean_text.replace('&lt;', '<')
        clean_text = clean_text.replace('&gt;', '>')
        clean_text = clean_text.replace('&amp;', '&')
        # Clean up whitespace
        clean_text = ' '.join(clean_text.split())
        return clean_text
    
    def scrape_posts(self, start_date: str, end_date: str, categories: List[str] = None) -> List[Dict]:
        """
        Scrape posts from specified categories within date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            categories: List of category names to scrape (default: all categories)
        
        Returns:
            List of scraped posts
        """
        # Parse dates
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)  # Include end date
        
        logger.info(f"Scraping posts from {start_date} to {end_date}")
        
        # Get categories
        all_categories = self.get_categories()
        target_categories = {}
        
        if categories:
            for cat_name in categories:
                if cat_name in all_categories:
                    target_categories[cat_name] = all_categories[cat_name]
                else:
                    logger.warning(f"Category '{cat_name}' not found")
        else:
            target_categories = all_categories
        
        logger.info(f"Target categories: {list(target_categories.keys())}")
        
        # Scrape posts from each category
        all_posts = []
        
        for cat_name, cat_id in target_categories.items():
            logger.info(f"Scraping category: {cat_name} (ID: {cat_id})")
            
            # Get topics for this category
            topics = self.get_topics_for_category(cat_id, start_dt, end_dt)
            
            # Get posts for each topic
            for topic in topics:
                topic_data = self.get_topic_posts(topic['id'])
                if topic_data:
                    # Add category name and URL to each post
                    for post in topic_data['posts']:
                        post['topic_title'] = topic_data['title']
                        post['category'] = cat_name
                        post['topic_url'] = f"{self.base_url}/t/{topic['id']}"
                        post['post_url'] = f"{self.base_url}/t/{topic['id']}/{post['post_number']}"
                        all_posts.append(post)
            
            logger.info(f"Scraped {len([p for p in all_posts if p['category'] == cat_name])} posts from {cat_name}")
        
        logger.info(f"Total posts scraped: {len(all_posts)}")
        return all_posts
    
    def save_to_json(self, posts: List[Dict], filename: str):
        """Save posts to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(posts, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Saved {len(posts)} posts to {filename}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
    
    def save_to_database(self, posts: List[Dict], db_path: str):
        """Save posts to SQLite database"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS discourse_posts (
                    id INTEGER PRIMARY KEY,
                    post_number INTEGER,
                    topic_title TEXT,
                    category TEXT,
                    username TEXT,
                    content TEXT,
                    raw_content TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    topic_url TEXT,
                    post_url TEXT,
                    reply_count INTEGER,
                    like_count INTEGER
                )
            ''')
            
            # Insert posts
            for post in posts:
                cursor.execute('''
                    INSERT OR REPLACE INTO discourse_posts 
                    (id, post_number, topic_title, category, username, content, raw_content,
                     created_at, updated_at, topic_url, post_url, reply_count, like_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post['id'], post['post_number'], post['topic_title'], post['category'],
                    post['username'], post['content'], post['raw_content'],
                    post['created_at'], post['updated_at'], post['topic_url'], post['post_url'],
                    post['reply_count'], post['like_count']
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved {len(posts)} posts to database: {db_path}")
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")

def main():
    parser = argparse.ArgumentParser(description='Scrape TDS Discourse posts')
    parser.add_argument('--url', required=True, help='Discourse base URL')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--api-key', help='Discourse API key (optional)')
    parser.add_argument('--username', help='Discourse username (optional)')
    parser.add_argument('--categories', nargs='+', help='Category names to scrape')
    parser.add_argument('--output-json', help='Output JSON file path')
    parser.add_argument('--db-path', help='SQLite database file path')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = TDSDiscourseScraper(args.url, args.api_key, args.username)
    
    # Scrape posts
    posts = scraper.scrape_posts(args.start_date, args.end_date, args.categories)
    
    # Save results
    if args.output_json:
        scraper.save_to_json(posts, args.output_json)
    
    if args.db_path:
        scraper.save_to_database(posts, args.db_path)
    
    if not args.output_json and not args.db_path:
        # Print summary if no output specified
        print(f"\nScraping Summary:")
        print(f"Total posts: {len(posts)}")
        print(f"Date range: {args.start_date} to {args.end_date}")
        
        # Group by category
        categories = {}
        for post in posts:
            cat = post['category']
            if cat not in categories:
                categories[cat] = 0
            categories[cat] += 1
        
        print("\nPosts by category:")
        for cat, count in categories.items():
            print(f"  {cat}: {count}")

if __name__ == "__main__":
    main()
