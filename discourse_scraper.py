#!/usr/bin/env python3
"""
Public Discourse Scraper - No Authentication Required
Scrapes TDS Discourse posts using public HTML endpoints and web scraping
"""

import argparse
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional
import re
from urllib.parse import urljoin, urlparse
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PublicDiscourseScraper:
    def __init__(self, base_url: str):
        """
        Initialize the public Discourse scraper
        
        Args:
            base_url: Base URL of the Discourse forum
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Set headers to mimic a regular browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def discover_tds_topics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Discover TDS-related topics by scraping public pages"""
        topics = []
        
        # Try different public endpoints
        endpoints_to_try = [
            '/latest',
            '/top',
            '/categories',
            '/',
        ]
        
        for endpoint in endpoints_to_try:
            try:
                logger.info(f"Trying endpoint: {endpoint}")
                page_topics = self._scrape_topics_from_page(endpoint, start_date, end_date)
                topics.extend(page_topics)
                
                if len(page_topics) > 0:
                    logger.info(f"Found {len(page_topics)} topics from {endpoint}")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error scraping {endpoint}: {e}")
                continue
        
        # Remove duplicates
        seen_ids = set()
        unique_topics = []
        for topic in topics:
            if topic['id'] not in seen_ids:
                unique_topics.append(topic)
                seen_ids.add(topic['id'])
        
        logger.info(f"Total unique topics found: {len(unique_topics)}")
        return unique_topics
    
    def _scrape_topics_from_page(self, endpoint: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Scrape topics from a specific page"""
        topics = []
        
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for topic links in various formats
            topic_selectors = [
                'a[href*="/t/"]',  # Standard topic links
                '.topic-list-item a',  # Topic list items
                '.topic-title a',  # Topic titles
                'tr.topic-list-item',  # Table rows
            ]
            
            for selector in topic_selectors:
                elements = soup.select(selector)
                
                for element in elements:
                    try:
                        topic_data = self._extract_topic_from_element(element, start_date, end_date)
                        if topic_data and self._is_tds_related(topic_data['title']):
                            topics.append(topic_data)
                    except Exception as e:
                        continue
            
            # Also try to find topics in JavaScript data
            script_topics = self._extract_topics_from_scripts(soup, start_date, end_date)
            topics.extend(script_topics)
            
        except Exception as e:
            logger.error(f"Error scraping page {endpoint}: {e}")
        
        return topics
    
    def _extract_topic_from_element(self, element, start_date: datetime, end_date: datetime) -> Optional[Dict]:
        """Extract topic data from HTML element"""
        try:
            # Get the link
            if element.name == 'a':
                link = element.get('href', '')
                title = element.get_text(strip=True)
            else:
                link_elem = element.find('a', href=re.compile(r'/t/'))
                if not link_elem:
                    return None
                link = link_elem.get('href', '')
                title = link_elem.get_text(strip=True)
            
            if not link or not title:
                return None
            
            # Extract topic ID from URL
            topic_id_match = re.search(r'/t/[^/]+/(\d+)', link)
            if not topic_id_match:
                return None
            
            topic_id = int(topic_id_match.group(1))
            
            # Try to find date information
            date_elem = element.find_parent().find(class_=re.compile(r'date|time|created')) if element.find_parent() else None
            created_at = None
            
            if date_elem:
                date_text = date_elem.get('title') or date_elem.get_text(strip=True)
                created_at = self._parse_date(date_text)
            
            # If no date found, we'll check it later by fetching the topic
            if not created_at:
                created_at = datetime.now()  # Placeholder
            
            return {
                'id': topic_id,
                'title': title,
                'url': urljoin(self.base_url, link),
                'created_at': created_at
            }
            
        except Exception as e:
            return None
    
    def _extract_topics_from_scripts(self, soup, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Extract topics from JavaScript data in page"""
        topics = []
        
        try:
            # Look for Discourse's JSON data in script tags
            script_tags = soup.find_all('script')
            
            for script in script_tags:
                if script.string and ('topic_list' in script.string or 'topics' in script.string):
                    # Try to extract JSON data
                    content = script.string
                    
                    # Look for topic data patterns
                    topic_matches = re.findall(r'"id":(\d+).*?"title":"([^"]+)".*?"created_at":"([^"]+)"', content)
                    
                    for match in topic_matches:
                        try:
                            topic_id = int(match[0])
                            title = match[1]
                            created_at_str = match[2]
                            
                            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                            created_at = created_at.replace(tzinfo=None)
                            
                            if start_date <= created_at <= end_date and self._is_tds_related(title):
                                topics.append({
                                    'id': topic_id,
                                    'title': title,
                                    'url': f"{self.base_url}/t/{topic_id}",
                                    'created_at': created_at
                                })
                        except:
                            continue
        except Exception as e:
            logger.warning(f"Error extracting from scripts: {e}")
        
        return topics
    
    def _is_tds_related(self, title: str) -> bool:
        """Check if topic title is related to Tools in Data Science"""
        if not title:
            return False
            
        title_lower = title.lower()
        
        # TDS-related keywords
        tds_keywords = [
            'tools', 'data', 'science', 'tds', 'assignment', 'project', 'graded',
            'python', 'pandas', 'numpy', 'matplotlib', 'jupyter', 'notebook',
            'ga1', 'ga2', 'ga3', 'ga4', 'ga5', 'ga6', 'ga7', 'ga8', 'ga9', 'ga10',
            'graded assignment', 'programming', 'coding', 'dataset', 'analysis',
            'visualization', 'machine learning', 'ml', 'statistics', 'csv',
            'dataframe', 'plot', 'graph', 'chart', 'regression', 'classification'
        ]
        
        # Check if any keyword is present
        return any(keyword in title_lower for keyword in tds_keywords)
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        if not date_str:
            return None
        
        # Common date formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except:
                continue
        
        # Try relative dates
        if 'ago' in date_str.lower():
            # Simple relative date parsing
            if 'hour' in date_str:
                hours = re.search(r'(\d+)', date_str)
                if hours:
                    return datetime.now() - timedelta(hours=int(hours.group(1)))
            elif 'day' in date_str:
                days = re.search(r'(\d+)', date_str)
                if days:
                    return datetime.now() - timedelta(days=int(days.group(1)))
        
        return None
    
    def scrape_topic_posts(self, topic_url: str) -> List[Dict]:
        """Scrape posts from a specific topic using HTML parsing"""
        posts = []
        
        try:
            response = self.session.get(topic_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract topic title
            title_elem = soup.find('h1') or soup.find(class_=re.compile(r'title'))
            topic_title = title_elem.get_text(strip=True) if title_elem else 'Unknown Title'
            
            # Find post elements
            post_selectors = [
                '.topic-post',
                '.post',
                'article[data-post-id]',
                '.cooked',
            ]
            
            post_elements = []
            for selector in post_selectors:
                elements = soup.select(selector)
                if elements:
                    post_elements = elements
                    break
            
            for i, post_elem in enumerate(post_elements):
                try:
                    post_data = self._extract_post_from_element(post_elem, topic_title, topic_url, i + 1)
                    if post_data:
                        posts.append(post_data)
                except Exception as e:
                    logger.warning(f"Error extracting post: {e}")
                    continue
            
            # If no posts found with structured parsing, try simpler extraction
            if not posts:
                posts = self._extract_posts_simple(soup, topic_title, topic_url)
            
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error scraping topic {topic_url}: {e}")
        
        return posts
    
    def _extract_post_from_element(self, post_elem, topic_title: str, topic_url: str, post_number: int) -> Optional[Dict]:
        """Extract post data from HTML element"""
        try:
            # Extract content
            content_elem = post_elem.find(class_=re.compile(r'cooked|content|post-content'))
            if not content_elem:
                content_elem = post_elem
            
            content = content_elem.get_text(strip=True)
            
            # Extract username
            username_elem = post_elem.find(class_=re.compile(r'username|author|user'))
            username = username_elem.get_text(strip=True) if username_elem else 'Unknown User'
            
            # Extract post ID if available
            post_id = post_elem.get('data-post-id') or post_elem.get('id')
            if post_id:
                post_id = re.search(r'\d+', str(post_id))
                post_id = int(post_id.group()) if post_id else post_number
            else:
                post_id = post_number
            
            # Extract date if available
            date_elem = post_elem.find(class_=re.compile(r'date|time|created'))
            created_at = None
            if date_elem:
                date_text = date_elem.get('title') or date_elem.get_text(strip=True)
                created_at = self._parse_date(date_text)
            
            if not created_at:
                created_at = datetime.now()
            
            return {
                'id': post_id,
                'post_number': post_number,
                'topic_title': topic_title,
                'category': 'Tools in Data Science',
                'username': username,
                'content': content,
                'raw_content': content,
                'created_at': created_at.isoformat(),
                'updated_at': created_at.isoformat(),
                'topic_url': topic_url,
                'post_url': f"{topic_url}/{post_number}",
                'reply_count': 0,
                'like_count': 0
            }
            
        except Exception as e:
            return None
    
    def _extract_posts_simple(self, soup, topic_title: str, topic_url: str) -> List[Dict]:
        """Simple post extraction as fallback"""
        posts = []
        
        try:
            # Find all text content that looks like posts
            text_elements = soup.find_all(['p', 'div'], string=re.compile(r'.{50,}'))  # At least 50 characters
            
            for i, elem in enumerate(text_elements[:20]):  # Limit to first 20 to avoid noise
                content = elem.get_text(strip=True)
                
                if len(content) > 50 and content not in [p.get('content', '') for p in posts]:
                    posts.append({
                        'id': i + 1,
                        'post_number': i + 1,
                        'topic_title': topic_title,
                        'category': 'Tools in Data Science',
                        'username': 'Unknown User',
                        'content': content,
                        'raw_content': content,
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat(),
                        'topic_url': topic_url,
                        'post_url': f"{topic_url}/{i + 1}",
                        'reply_count': 0,
                        'like_count': 0
                    })
        except Exception as e:
            logger.warning(f"Error in simple extraction: {e}")
        
        return posts
    
    def scrape_posts(self, start_date: str, end_date: str) -> List[Dict]:
        """Main scraping method"""
        # Parse dates
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        
        logger.info(f"Scraping TDS posts from {start_date} to {end_date}")
        logger.info("Using public HTML scraping (no authentication required)")
        
        # Discover TDS topics
        topics = self.discover_tds_topics(start_dt, end_dt)
        
        if not topics:
            logger.warning("No topics found. The forum might be fully private or have different structure.")
            return []
        
        # Scrape posts from each topic
        all_posts = []
        
        for topic in topics:
            logger.info(f"Scraping topic: {topic['title']}")
            posts = self.scrape_topic_posts(topic['url'])
            all_posts.extend(posts)
            
            time.sleep(1)  # Rate limiting between topics
        
        # Filter posts by date
        filtered_posts = []
        for post in all_posts:
            try:
                post_date = datetime.fromisoformat(post['created_at'].replace('Z', ''))
                if start_dt <= post_date <= end_dt:
                    filtered_posts.append(post)
            except:
                # Include posts where we can't parse the date
                filtered_posts.append(post)
        
        logger.info(f"Total posts scraped: {len(filtered_posts)}")
        return filtered_posts
    
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
    parser = argparse.ArgumentParser(description='Scrape TDS Discourse posts (no auth required)')
    parser.add_argument('--url', required=True, help='Discourse base URL')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-json', help='Output JSON file path')
    parser.add_argument('--db-path', help='SQLite database file path')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = PublicDiscourseScraper(args.url)
    
    # Scrape posts
    posts = scraper.scrape_posts(args.start_date, args.end_date)
    
    # Save results
    if args.output_json:
        scraper.save_to_json(posts, args.output_json)
    
    if args.db_path:
        scraper.save_to_database(posts, args.db_path)
    
    if not args.output_json and not args.db_path:
        print(f"\nScraping Summary:")
        print(f"Total posts: {len(posts)}")
        print(f"Date range: {args.start_date} to {args.end_date}")
        
        if posts:
            print("\nSample topics found:")
            topics = list(set([post['topic_title'] for post in posts[:10]]))
            for topic in topics:
                print(f"  - {topic}")

if __name__ == "__main__":
    main()
