#!/usr/bin/env python3
"""
Production-Ready TDS Discourse Scraper
Scrapes TDS Discourse posts with robust error handling, retry mechanisms, and data validation
"""

import argparse
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Optional, Set
import re
from urllib.parse import urljoin, urlparse
import sqlite3
import hashlib
import yaml
from pathlib import Path
import signal
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discourse_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DiscourseScraperConfig:
    """Configuration management for the scraper"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.config = self._load_default_config()
        if config_file and Path(config_file).exists():
            self._load_config_file(config_file)
    
    def _load_default_config(self) -> Dict:
        return {
            'scraping': {
                'rate_limit_delay': 1.0,
                'request_timeout': 30,
                'max_retries': 3,
                'retry_backoff': 2.0,
                'max_workers': 5,
                'user_agent': 'TDS-Research-Bot/1.0 (Educational Use)',
                'max_posts_per_topic': 1000,
                'max_topics': 500
            },
            'tds_keywords': {
                'primary': ['tools', 'data', 'science', 'tds', 'assignment', 'graded'],
                'secondary': ['python', 'pandas', 'numpy', 'matplotlib', 'jupyter', 'notebook'],
                'assignments': ['ga1', 'ga2', 'ga3', 'ga4', 'ga5', 'ga6', 'ga7', 'ga8', 'ga9', 'ga10'],
                'technical': ['programming', 'coding', 'dataset', 'analysis', 'visualization', 
                            'machine learning', 'ml', 'statistics', 'csv', 'dataframe', 
                            'plot', 'graph', 'chart', 'regression', 'classification']
            },
            'data_validation': {
                'min_content_length': 10,
                'max_content_length': 50000,
                'exclude_patterns': ['[deleted]', '[removed]', 'this post was flagged'],
                'min_word_count': 3
            }
        }
    
    def _load_config_file(self, config_file: str):
        """Load configuration from YAML file"""
        try:
            with open(config_file, 'r') as f:
                file_config = yaml.safe_load(f)
                self._merge_config(self.config, file_config)
        except Exception as e:
            logger.error(f"Error in HTML scraping for topic {topic['id']}: {e}")
            return []
    
    def _find_post_elements(self, soup: BeautifulSoup) -> List:
        """Find post elements using multiple selectors"""
        post_selectors = [
            '.topic-post',
            '.post',
            'article[data-post-id]',
            '.cooked',
            '[data-post-number]'
        ]
        
        for selector in post_selectors:
            elements = soup.select(selector)
            if elements:
                return elements
        
        return []
    
    def _extract_post_from_html(self, post_elem, topic: Dict, post_number: int) -> Optional[Dict]:
        """Extract post data from HTML element"""
        try:
            # Extract content
            content_selectors = ['.cooked', '.post-content', '.content']
            content = ""
            
            for selector in content_selectors:
                content_elem = post_elem.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    break
            
            if not content:
                content = post_elem.get_text(strip=True)
            
            # Extract username
            username_selectors = ['.username', '.author', '.user-name']
            username = "Unknown"
            
            for selector in username_selectors:
                username_elem = post_elem.select_one(selector)
                if username_elem:
                    username = username_elem.get_text(strip=True)
                    break
            
            # Extract post ID
            post_id = post_elem.get('data-post-id') or post_elem.get('id')
            if post_id:
                post_id_match = re.search(r'\d+', str(post_id))
                post_id = int(post_id_match.group()) if post_id_match else post_number
            else:
                post_id = post_number
            
            # Extract date
            date_selectors = ['.post-date', '.date', '.created-at']
            created_at = None
            
            for selector in date_selectors:
                date_elem = post_elem.select_one(selector)
                if date_elem:
                    date_text = date_elem.get('title') or date_elem.get('data-time') or date_elem.get_text(strip=True)
                    created_at = date_text
                    break
            
            if not created_at:
                created_at = datetime.now().isoformat()
            
            return {
                'id': post_id,
                'post_number': post_number,
                'topic_title': topic['title'],
                'category': 'Tools in Data Science',
                'username': username,
                'content': content,
                'raw_content': content,
                'created_at': created_at,
                'updated_at': created_at,
                'topic_url': topic['url'],
                'post_url': f"{topic['url']}/{post_number}",
                'reply_count': 0,
                'like_count': 0
            }
            
        except Exception as e:
            logger.debug(f"Error extracting post from HTML: {e}")
            return None
    
    def scrape_posts_parallel(self, topics: List[Dict]) -> List[Dict]:
        """Scrape posts from multiple topics in parallel"""
        all_posts = []
        max_workers = self.config.get('scraping.max_workers', 5)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_topic = {
                executor.submit(self.scrape_topic_posts, topic): topic 
                for topic in topics
            }
            
            # Process completed tasks
            for future in as_completed(future_to_topic):
                if self._shutdown_requested:
                    break
                
                topic = future_to_topic[future]
                try:
                    posts = future.result(timeout=300)  # 5 minute timeout per topic
                    all_posts.extend(posts)
                except Exception as e:
                    logger.error(f"Error processing topic {topic['id']}: {e}")
                    continue
        
        return all_posts
    
    def scrape_posts(self, start_date: str, end_date: str, parallel: bool = True) -> List[Dict]:
        """Main scraping method"""
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            
            logger.info(f"Starting TDS scraping from {start_date} to {end_date}")
            logger.info(f"Target URL: {self.base_url}")
            
            # Discover topics
            topics = self.discover_topics(start_dt, end_dt)
            
            if not topics:
                logger.warning("No TDS topics found for the specified date range")
                return []
            
            logger.info(f"Found {len(topics)} TDS topics to scrape")
            
            # Scrape posts
            if parallel:
                all_posts = self.scrape_posts_parallel(topics)
            else:
                all_posts = []
                for topic in topics:
                    if self._shutdown_requested:
                        break
                    posts = self.scrape_topic_posts(topic)
                    all_posts.extend(posts)
            
            # Final filtering by date
            filtered_posts = self._filter_posts_by_date(all_posts, start_dt, end_dt)
            
            logger.info(f"Scraping completed. Total posts: {len(filtered_posts)}")
            return filtered_posts
            
        except Exception as e:
            logger.error(f"Error in main scraping process: {e}")
            return []
    
    def _filter_posts_by_date(self, posts: List[Dict], start_dt: datetime, end_dt: datetime) -> List[Dict]:
        """Filter posts by date range"""
        filtered_posts = []
        
        for post in posts:
            try:
                post_date_str = post.get('created_at', '')
                post_date = self.validator._parse_date(post_date_str)
                
                if post_date and start_dt <= post_date <= end_dt:
                    filtered_posts.append(post)
                elif not post_date:
                    # Include posts where we can't determine the date
                    filtered_posts.append(post)
                    
            except Exception as e:
                # Include posts that have date parsing errors
                filtered_posts.append(post)
        
        return filtered_posts
    
    def save_to_json(self, posts: List[Dict], filename: str):
        """Save posts to JSON file with error handling"""
        try:
            # Ensure directory exists
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            
            # Add metadata
            output_data = {
                'metadata': {
                    'scrape_date': datetime.now().isoformat(),
                    'base_url': self.base_url,
                    'total_posts': len(posts),
                    'scraper_version': '2.0.0'
                },
                'posts': posts
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Successfully saved {len(posts)} posts to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            raise
    
    def save_to_database(self, posts: List[Dict], db_path: str):
        """Save posts to SQLite database with improved schema"""
        try:
            # Ensure directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create improved schema
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS discourse_posts (
                    id INTEGER PRIMARY KEY,
                    post_number INTEGER,
                    topic_title TEXT NOT NULL,
                    category TEXT,
                    username TEXT NOT NULL,
                    content TEXT NOT NULL,
                    raw_content TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    topic_url TEXT,
                    post_url TEXT,
                    reply_count INTEGER DEFAULT 0,
                    like_count INTEGER DEFAULT 0,
                    content_hash TEXT,
                    scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(id, post_number)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_created_at ON discourse_posts(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_topic_title ON discourse_posts(topic_title)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_username ON discourse_posts(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_posts_content_hash ON discourse_posts(content_hash)')
            
            # Create metadata table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrape_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scrape_date TEXT,
                    base_url TEXT,
                    total_posts INTEGER,
                    scraper_version TEXT
                )
            ''')
            
            # Insert metadata
            cursor.execute('''
                INSERT INTO scrape_metadata (scrape_date, base_url, total_posts, scraper_version)
                VALUES (?, ?, ?, ?)
            ''', (datetime.now().isoformat(), self.base_url, len(posts), '2.0.0'))
            
            # Insert posts
            for post in posts:
                content_hash = hashlib.md5(post['content'].encode()).hexdigest()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO discourse_posts 
                    (id, post_number, topic_title, category, username, content, raw_content,
                     created_at, updated_at, topic_url, post_url, reply_count, like_count, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post['id'], post['post_number'], post['topic_title'], post['category'],
                    post['username'], post['content'], post['raw_content'],
                    post['created_at'], post['updated_at'], post['topic_url'], post['post_url'],
                    post['reply_count'], post['like_count'], content_hash
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully saved {len(posts)} posts to database: {db_path}")
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            raise
    
    def generate_report(self, posts: List[Dict]) -> Dict:
        """Generate a comprehensive scraping report"""
        if not posts:
            return {'error': 'No posts to analyze'}
        
        try:
            # Basic statistics
            total_posts = len(posts)
            unique_topics = len(set(post['topic_title'] for post in posts))
            unique_users = len(set(post['username'] for post in posts))
            
            # Date analysis
            dates = []
            for post in posts:
                try:
                    post_date = self.validator._parse_date(post['created_at'])
                    if post_date:
                        dates.append(post_date)
                except:
                    continue
            
            date_range = None
            if dates:
                dates.sort()
                date_range = {
                    'earliest': dates[0].isoformat(),
                    'latest': dates[-1].isoformat()
                }
            
            # Content analysis
            total_content_length = sum(len(post['content']) for post in posts)
            avg_content_length = total_content_length / total_posts if total_posts > 0 else 0
            
            # User activity
            user_post_counts = {}
            for post in posts:
                username = post['username']
                user_post_counts[username] = user_post_counts.get(username, 0) + 1
            
            top_users = sorted(user_post_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            # Topic analysis
            topic_post_counts = {}
            for post in posts:
                topic = post['topic_title']
                topic_post_counts[topic] = topic_post_counts.get(topic, 0) + 1
            
            top_topics = sorted(topic_post_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            # TDS keyword analysis
            keyword_counts = {}
            all_keywords = (
                self.keyword_matcher.primary_keywords | 
                self.keyword_matcher.secondary_keywords | 
                self.keyword_matcher.assignment_keywords | 
                self.keyword_matcher.technical_keywords
            )
            
            for keyword in all_keywords:
                count = sum(1 for post in posts if keyword in post['content'].lower())
                if count > 0:
                    keyword_counts[keyword] = count
            
            top_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            
            return {
                'summary': {
                    'total_posts': total_posts,
                    'unique_topics': unique_topics,
                    'unique_users': unique_users,
                    'date_range': date_range,
                    'avg_content_length': round(avg_content_length, 2),
                    'total_content_length': total_content_length
                },
                'top_users': top_users,
                'top_topics': top_topics,
                'top_keywords': top_keywords,
                'scraping_info': {
                    'base_url': self.base_url,
                    'scraper_version': '2.0.0',
                    'scrape_timestamp': datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {'error': f'Report generation failed: {e}'}

def create_sample_config():
    """Create a sample configuration file"""
    config = {
        'scraping': {
            'rate_limit_delay': 1.0,
            'request_timeout': 30,
            'max_retries': 3,
            'retry_backoff': 2.0,
            'max_workers': 3,
            'user_agent': 'TDS-Research-Bot/1.0 (Educational Use)',
            'max_posts_per_topic': 1000,
            'max_topics': 200
        },
        'tds_keywords': {
            'primary': ['tools', 'data', 'science', 'tds', 'assignment', 'graded'],
            'secondary': ['python', 'pandas', 'numpy', 'matplotlib', 'jupyter', 'notebook'],
            'assignments': ['ga1', 'ga2', 'ga3', 'ga4', 'ga5', 'ga6', 'ga7', 'ga8', 'ga9', 'ga10'],
            'technical': ['programming', 'coding', 'dataset', 'analysis', 'visualization', 
                        'machine learning', 'ml', 'statistics', 'csv', 'dataframe', 
                        'plot', 'graph', 'chart', 'regression', 'classification']
        },
        'data_validation': {
            'min_content_length': 20,
            'max_content_length': 50000,
            'exclude_patterns': ['[deleted]', '[removed]', 'this post was flagged'],
            'min_word_count': 5
        }
    }
    
    with open('tds_scraper_config.yaml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print("Sample configuration file created: tds_scraper_config.yaml")

def main():
    """Main entry point with comprehensive argument parsing"""
    parser = argparse.ArgumentParser(
        description='Production-Ready TDS Discourse Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python tds_scraper.py --url https://discourse.example.com --start-date 2024-01-01 --end-date 2024-01-31 --output-json results.json
  
  # With database output and custom config
  python tds_scraper.py --url https://discourse.example.com --start-date 2024-01-01 --end-date 2024-01-31 --db-path results.db --config config.yaml
  
  # Generate sample configuration
  python tds_scraper.py --create-config
  
  # Sequential scraping (slower but more reliable)
  python tds_scraper.py --url https://discourse.example.com --start-date 2024-01-01 --end-date 2024-01-31 --output-json results.json --sequential
        """
    )
    
    parser.add_argument('--url', help='Discourse base URL')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-json', help='Output JSON file path')
    parser.add_argument('--db-path', help='SQLite database file path')
    parser.add_argument('--config', help='Configuration file path (YAML)')
    parser.add_argument('--sequential', action='store_true', help='Use sequential scraping instead of parallel')
    parser.add_argument('--create-config', action='store_true', help='Create sample configuration file')
    parser.add_argument('--report', help='Generate scraping report to specified file')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Set logging level')
    parser.add_argument('--version', action='version', version='TDS Discourse Scraper 2.0.0')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Handle config creation
    if args.create_config:
        create_sample_config()
        return
    
    # Validate required arguments
    if not args.url or not args.start_date or not args.end_date:
        parser.error("--url, --start-date, and --end-date are required (unless using --create-config)")
    
    if not args.output_json and not args.db_path:
        parser.error("Either --output-json or --db-path must be specified")
    
    # Validate date format
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        parser.error("Dates must be in YYYY-MM-DD format")
    
    try:
        # Initialize scraper
        logger.info("Initializing TDS Discourse Scraper v2.0.0")
        scraper = ProductionDiscourseScraper(args.url, args.config)
        
        # Scrape posts
        posts = scraper.scrape_posts(args.start_date, args.end_date, parallel=not args.sequential)
        
        if not posts:
            logger.warning("No posts were scraped. Check your configuration and try again.")
            return
        
        # Save results
        if args.output_json:
            scraper.save_to_json(posts, args.output_json)
        
        if args.db_path:
            scraper.save_to_database(posts, args.db_path)
        
        # Generate report
        report = scraper.generate_report(posts)
        
        if args.report:
            with open(args.report, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report saved to {args.report}")
        else:
            # Print summary to console
            print("\n" + "="*50)
            print("SCRAPING SUMMARY")
            print("="*50)
            
            if 'summary' in report:
                summary = report['summary']
                print(f"Total Posts: {summary['total_posts']}")
                print(f"Unique Topics: {summary['unique_topics']}")
                print(f"Unique Users: {summary['unique_users']}")
                print(f"Average Content Length: {summary['avg_content_length']} characters")
                
                if summary['date_range']:
                    print(f"Date Range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
                
                if 'top_topics' in report and report['top_topics']:
                    print(f"\nTop Topics:")
                    for topic, count in report['top_topics'][:5]:
                        print(f"  - {topic[:60]}... ({count} posts)")
                
                if 'top_keywords' in report and report['top_keywords']:
                    print(f"\nTop TDS Keywords:")
                    for keyword, count in report['top_keywords'][:10]:
                        print(f"  - {keyword}: {count} mentions")
            
            print("="*50)
        
        logger.info("Scraping completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure required packages are available
    try:
        import yaml
        import requests
        from bs4 import BeautifulSoup
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Please install required packages:")
        print("pip install requests beautifulsoup4 pyyaml")
        sys.exit(1)
    
    main()
            logger.warning(f"Could not load config file {config_file}: {e}")
    
    def _merge_config(self, base: Dict, override: Dict):
        """Recursively merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def get(self, path: str, default=None):
        """Get configuration value using dot notation"""
        keys = path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

class DataValidator:
    """Validates and cleans scraped data"""
    
    def __init__(self, config: DiscourseScraperConfig):
        self.config = config
        self.seen_hashes: Set[str] = set()
    
    def validate_post(self, post: Dict) -> bool:
        """Validate a single post"""
        try:
            # Check required fields
            required_fields = ['content', 'username', 'topic_title']
            if not all(field in post and post[field] for field in required_fields):
                return False
            
            # Check content length
            content = post['content'].strip()
            min_len = self.config.get('data_validation.min_content_length', 10)
            max_len = self.config.get('data_validation.max_content_length', 50000)
            
            if len(content) < min_len or len(content) > max_len:
                return False
            
            # Check word count
            word_count = len(content.split())
            min_words = self.config.get('data_validation.min_word_count', 3)
            if word_count < min_words:
                return False
            
            # Check for excluded patterns
            exclude_patterns = self.config.get('data_validation.exclude_patterns', [])
            content_lower = content.lower()
            if any(pattern.lower() in content_lower for pattern in exclude_patterns):
                return False
            
            # Check for duplicates
            content_hash = hashlib.md5(content.encode()).hexdigest()
            if content_hash in self.seen_hashes:
                return False
            
            self.seen_hashes.add(content_hash)
            return True
            
        except Exception as e:
            logger.warning(f"Error validating post: {e}")
            return False
    
    def clean_post(self, post: Dict) -> Dict:
        """Clean and normalize post data"""
        try:
            # Clean content
            post['content'] = self._clean_text(post['content'])
            post['raw_content'] = post['content']  # Keep original
            
            # Normalize username
            post['username'] = self._clean_text(post['username'])
            
            # Normalize topic title
            post['topic_title'] = self._clean_text(post['topic_title'])
            
            # Ensure numeric fields
            post['reply_count'] = int(post.get('reply_count', 0))
            post['like_count'] = int(post.get('like_count', 0))
            post['post_number'] = int(post.get('post_number', 1))
            
            # Validate and normalize dates
            post['created_at'] = self._normalize_date(post.get('created_at'))
            post['updated_at'] = self._normalize_date(post.get('updated_at'))
            
            return post
            
        except Exception as e:
            logger.warning(f"Error cleaning post: {e}")
            return post
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove control characters
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        return text
    
    def _normalize_date(self, date_str: str) -> str:
        """Normalize date string to ISO format"""
        if not date_str:
            return datetime.now().isoformat()
        
        try:
            # If already ISO format, return as is
            datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return date_str
        except:
            # Try to parse and convert
            parsed_date = self._parse_date(date_str)
            return parsed_date.isoformat() if parsed_date else datetime.now().isoformat()
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        if not date_str:
            return None
        
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except:
                continue
        
        # Handle relative dates
        return self._parse_relative_date(date_str)
    
    def _parse_relative_date(self, date_str: str) -> Optional[datetime]:
        """Parse relative date strings"""
        try:
            now = datetime.now()
            date_lower = date_str.lower()
            
            if 'ago' in date_lower:
                if 'minute' in date_lower:
                    minutes = re.search(r'(\d+)', date_str)
                    if minutes:
                        return now - timedelta(minutes=int(minutes.group(1)))
                elif 'hour' in date_lower:
                    hours = re.search(r'(\d+)', date_str)
                    if hours:
                        return now - timedelta(hours=int(hours.group(1)))
                elif 'day' in date_lower:
                    days = re.search(r'(\d+)', date_str)
                    if days:
                        return now - timedelta(days=int(days.group(1)))
                elif 'week' in date_lower:
                    weeks = re.search(r'(\d+)', date_str)
                    if weeks:
                        return now - timedelta(weeks=int(weeks.group(1)))
                elif 'month' in date_lower:
                    months = re.search(r'(\d+)', date_str)
                    if months:
                        return now - timedelta(days=int(months.group(1)) * 30)
            
            return None
        except:
            return None

class RobustHTTPSession:
    """HTTP session with retry logic and rate limiting"""
    
    def __init__(self, config: DiscourseScraperConfig):
        self.config = config
        self.session = requests.Session()
        self._setup_session()
        self.last_request_time = 0
        self.request_lock = threading.Lock()
    
    def _setup_session(self):
        """Configure session with retry strategy"""
        retry_strategy = Retry(
            total=self.config.get('scraping.max_retries', 3),
            backoff_factor=self.config.get('scraping.retry_backoff', 2.0),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set headers
        self.session.headers.update({
            'User-Agent': self.config.get('scraping.user_agent'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Make GET request with rate limiting"""
        with self.request_lock:
            # Rate limiting
            rate_limit = self.config.get('scraping.rate_limit_delay', 1.0)
            time_since_last = time.time() - self.last_request_time
            if time_since_last < rate_limit:
                time.sleep(rate_limit - time_since_last)
            
            timeout = kwargs.pop('timeout', self.config.get('scraping.request_timeout', 30))
            
            try:
                response = self.session.get(url, timeout=timeout, **kwargs)
                self.last_request_time = time.time()
                return response
            except Exception as e:
                logger.error(f"Request failed for {url}: {e}")
                raise

class TDSKeywordMatcher:
    """Advanced TDS keyword matching with scoring"""
    
    def __init__(self, config: DiscourseScraperConfig):
        self.config = config
        self.primary_keywords = set(kw.lower() for kw in config.get('tds_keywords.primary', []))
        self.secondary_keywords = set(kw.lower() for kw in config.get('tds_keywords.secondary', []))
        self.assignment_keywords = set(kw.lower() for kw in config.get('tds_keywords.assignments', []))
        self.technical_keywords = set(kw.lower() for kw in config.get('tds_keywords.technical', []))
    
    def is_tds_related(self, title: str, content: str = "") -> bool:
        """Check if content is TDS-related using weighted scoring"""
        if not title:
            return False
        
        score = self._calculate_tds_score(title, content)
        return score >= 1.0  # Threshold for TDS relevance
    
    def _calculate_tds_score(self, title: str, content: str = "") -> float:
        """Calculate TDS relevance score"""
        title_lower = title.lower()
        content_lower = content.lower()
        combined_text = f"{title_lower} {content_lower}"
        
        score = 0.0
        
        # Primary keywords (high weight)
        for keyword in self.primary_keywords:
            if keyword in title_lower:
                score += 2.0
            elif keyword in content_lower:
                score += 1.0
        
        # Assignment keywords (very high weight)
        for keyword in self.assignment_keywords:
            if keyword in combined_text:
                score += 3.0
        
        # Secondary keywords (medium weight)
        for keyword in self.secondary_keywords:
            if keyword in combined_text:
                score += 0.5
        
        # Technical keywords (low weight, but cumulative)
        technical_matches = sum(1 for keyword in self.technical_keywords if keyword in combined_text)
        score += min(technical_matches * 0.3, 2.0)  # Cap at 2.0
        
        return score

class ProductionDiscourseScraper:
    """Production-ready Discourse scraper with robust error handling"""
    
    def __init__(self, base_url: str, config_file: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.config = DiscourseScraperConfig(config_file)
        self.session = RobustHTTPSession(self.config)
        self.validator = DataValidator(self.config)
        self.keyword_matcher = TDSKeywordMatcher(self.config)
        self.scraped_topics: Set[int] = set()
        self.scraped_posts: Set[str] = set()
        self._shutdown_requested = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._shutdown_requested = True
    
    def discover_topics(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Discover TDS-related topics with pagination support"""
        logger.info("Discovering TDS topics...")
        topics = []
        
        # Try different discovery methods
        discovery_methods = [
            self._discover_from_latest,
            self._discover_from_categories,
            self._discover_from_search,
            self._discover_from_json_api
        ]
        
        for method in discovery_methods:
            if self._shutdown_requested:
                break
                
            try:
                logger.info(f"Trying discovery method: {method.__name__}")
                method_topics = method(start_date, end_date)
                topics.extend(method_topics)
                
                if method_topics:
                    logger.info(f"Found {len(method_topics)} topics using {method.__name__}")
                
            except Exception as e:
                logger.warning(f"Discovery method {method.__name__} failed: {e}")
                continue
        
        # Remove duplicates and validate
        unique_topics = self._deduplicate_topics(topics)
        validated_topics = []
        
        for topic in unique_topics:
            if self.keyword_matcher.is_tds_related(topic['title']):
                validated_topics.append(topic)
        
        logger.info(f"Total validated TDS topics: {len(validated_topics)}")
        return validated_topics[:self.config.get('scraping.max_topics', 500)]
    
    def _discover_from_latest(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Discover topics from latest page"""
        topics = []
        page = 0
        
        while page < 10 and not self._shutdown_requested:  # Limit pages
            try:
                url = f"{self.base_url}/latest"
                if page > 0:
                    url += f"?page={page}"
                
                response = self.session.get(url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                page_topics = self._extract_topics_from_soup(soup, start_date, end_date)
                
                if not page_topics:
                    break
                
                topics.extend(page_topics)
                page += 1
                
            except Exception as e:
                logger.warning(f"Error in latest page {page}: {e}")
                break
        
        return topics
    
    def _discover_from_categories(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Discover topics from categories page"""
        topics = []
        
        try:
            response = self.session.get(f"{self.base_url}/categories")
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find category links
            category_links = soup.find_all('a', href=re.compile(r'/c/'))
            
            for link in category_links[:5]:  # Limit categories
                if self._shutdown_requested:
                    break
                
                try:
                    category_url = urljoin(self.base_url, link.get('href'))
                    category_response = self.session.get(category_url)
                    category_response.raise_for_status()
                    
                    category_soup = BeautifulSoup(category_response.content, 'html.parser')
                    category_topics = self._extract_topics_from_soup(category_soup, start_date, end_date)
                    topics.extend(category_topics)
                    
                except Exception as e:
                    logger.warning(f"Error scraping category {link.get('href')}: {e}")
                    continue
        
        except Exception as e:
            logger.warning(f"Error in categories discovery: {e}")
        
        return topics
    
    def _discover_from_search(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Discover topics using search functionality"""
        topics = []
        
        # Search for TDS-related terms
        search_terms = ['TDS', 'tools data science', 'graded assignment', 'GA1', 'GA2']
        
        for term in search_terms:
            if self._shutdown_requested:
                break
                
            try:
                search_url = f"{self.base_url}/search?q={term}"
                response = self.session.get(search_url)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                search_topics = self._extract_topics_from_soup(soup, start_date, end_date)
                topics.extend(search_topics)
                
            except Exception as e:
                logger.warning(f"Error searching for '{term}': {e}")
                continue
        
        return topics
    
    def _discover_from_json_api(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Try to discover topics using JSON API endpoints"""
        topics = []
        
        json_endpoints = [
            '/latest.json',
            '/top.json',
            '/categories.json'
        ]
        
        for endpoint in json_endpoints:
            if self._shutdown_requested:
                break
                
            try:
                response = self.session.get(f"{self.base_url}{endpoint}")
                response.raise_for_status()
                
                data = response.json()
                json_topics = self._extract_topics_from_json(data, start_date, end_date)
                topics.extend(json_topics)
                
            except Exception as e:
                logger.warning(f"Error with JSON endpoint {endpoint}: {e}")
                continue
        
        return topics
    
    def _extract_topics_from_soup(self, soup: BeautifulSoup, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Extract topics from BeautifulSoup object"""
        topics = []
        
        # Multiple selectors for topic links
        selectors = [
            'a[href*="/t/"]',
            '.topic-list-item a',
            '.topic-title a',
            'tr.topic-list-item a'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            
            for element in elements:
                try:
                    topic = self._extract_topic_from_element(element)
                    if topic and self._is_date_in_range(topic.get('created_at'), start_date, end_date):
                        topics.append(topic)
                except Exception as e:
                    continue
        
        return topics
    
    def _extract_topics_from_json(self, data: Dict, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Extract topics from JSON data"""
        topics = []
        
        try:
            # Handle different JSON structures
            topic_lists = []
            
            if 'topic_list' in data and 'topics' in data['topic_list']:
                topic_lists.append(data['topic_list']['topics'])
            
            if 'topics' in data:
                topic_lists.append(data['topics'])
            
            for topic_list in topic_lists:
                for topic_data in topic_list:
                    try:
                        topic = {
                            'id': topic_data.get('id'),
                            'title': topic_data.get('title', ''),
                            'slug': topic_data.get('slug', ''),
                            'created_at': topic_data.get('created_at'),
                            'url': f"{self.base_url}/t/{topic_data.get('slug', '')}/{topic_data.get('id', '')}"
                        }
                        
                        if self._is_date_in_range(topic.get('created_at'), start_date, end_date):
                            topics.append(topic)
                            
                    except Exception as e:
                        continue
        
        except Exception as e:
            logger.warning(f"Error extracting from JSON: {e}")
        
        return topics
    
    def _extract_topic_from_element(self, element) -> Optional[Dict]:
        """Extract topic data from HTML element"""
        try:
            href = element.get('href', '')
            if not href or '/t/' not in href:
                return None
            
            # Extract topic ID
            topic_id_match = re.search(r'/t/[^/]+/(\d+)', href)
            if not topic_id_match:
                return None
            
            topic_id = int(topic_id_match.group(1))
            
            if topic_id in self.scraped_topics:
                return None
            
            title = element.get_text(strip=True)
            if not title:
                return None
            
            # Try to find date information
            created_at = self._find_date_near_element(element)
            
            return {
                'id': topic_id,
                'title': title,
                'url': urljoin(self.base_url, href),
                'created_at': created_at
            }
            
        except Exception as e:
            return None
    
    def _find_date_near_element(self, element) -> Optional[str]:
        """Find date information near the topic element"""
        try:
            # Look for date in parent elements
            current = element
            for _ in range(5):  # Check up to 5 parent levels
                if not current:
                    break
                
                # Look for date-related elements
                date_selectors = [
                    '[data-time]',
                    '.date',
                    '.time',
                    '.created-at',
                    '.activity-date'
                ]
                
                for selector in date_selectors:
                    date_elem = current.select_one(selector)
                    if date_elem:
                        date_text = date_elem.get('data-time') or date_elem.get('title') or date_elem.get_text(strip=True)
                        if date_text:
                            return date_text
                
                current = current.parent
            
            return None
            
        except Exception as e:
            return None
    
    def _is_date_in_range(self, date_str: str, start_date: datetime, end_date: datetime) -> bool:
        """Check if date string is within the specified range"""
        if not date_str:
            return True  # Include if we can't determine date
        
        try:
            parsed_date = self.validator._parse_date(date_str)
            if not parsed_date:
                return True
            
            return start_date <= parsed_date <= end_date
        except:
            return True
    
    def _deduplicate_topics(self, topics: List[Dict]) -> List[Dict]:
        """Remove duplicate topics"""
        seen_ids = set()
        unique_topics = []
        
        for topic in topics:
            topic_id = topic.get('id')
            if topic_id and topic_id not in seen_ids:
                unique_topics.append(topic)
                seen_ids.add(topic_id)
        
        return unique_topics
    
    def scrape_topic_posts(self, topic: Dict) -> List[Dict]:
        """Scrape posts from a specific topic with pagination"""
        if self._shutdown_requested:
            return []
        
        posts = []
        topic_id = topic['id']
        
        if topic_id in self.scraped_topics:
            return []
        
        try:
            logger.info(f"Scraping topic: {topic['title'][:50]}...")
            
            # Try JSON API first
            json_posts = self._scrape_topic_json(topic)
            if json_posts:
                posts.extend(json_posts)
            else:
                # Fallback to HTML scraping
                html_posts = self._scrape_topic_html(topic)
                posts.extend(html_posts)
            
            # Validate and clean posts
            valid_posts = []
            for post in posts:
                if self.validator.validate_post(post):
                    cleaned_post = self.validator.clean_post(post)
                    valid_posts.append(cleaned_post)
            
            self.scraped_topics.add(topic_id)
            logger.info(f"Scraped {len(valid_posts)} valid posts from topic {topic_id}")
            
            return valid_posts
            
        except Exception as e:
            logger.error(f"Error scraping topic {topic_id}: {e}")
            return []
    
    def _scrape_topic_json(self, topic: Dict) -> List[Dict]:
        """Try to scrape topic using JSON API"""
        try:
            topic_id = topic['id']
            json_url = f"{self.base_url}/t/{topic_id}.json"
            
            response = self.session.get(json_url)
            response.raise_for_status()
            
            data = response.json()
            posts = []
            
            if 'post_stream' in data and 'posts' in data['post_stream']:
                for post_data in data['post_stream']['posts']:
                    post = self._convert_json_post(post_data, topic)
                    if post:
                        posts.append(post)
            
            return posts
            
        except Exception as e:
            logger.debug(f"JSON API failed for topic {topic['id']}: {e}")
            return []
    
    def _convert_json_post(self, post_data: Dict, topic: Dict) -> Optional[Dict]:
        """Convert JSON post data to standard format"""
        try:
            return {
                'id': post_data.get('id'),
                'post_number': post_data.get('post_number', 1),
                'topic_title': topic['title'],
                'category': 'Tools in Data Science',
                'username': post_data.get('username', 'Unknown'),
                'content': post_data.get('cooked', ''),
                'raw_content': post_data.get('raw', ''),
                'created_at': post_data.get('created_at'),
                'updated_at': post_data.get('updated_at'),
                'topic_url': topic['url'],
                'post_url': f"{topic['url']}/{post_data.get('post_number', 1)}",
                'reply_count': post_data.get('reply_count', 0),
                'like_count': post_data.get('score', 0)
            }
        except Exception as e:
            return None
    
    def _scrape_topic_html(self, topic: Dict) -> List[Dict]:
        """Scrape topic using HTML parsing"""
        posts = []
        
        try:
            response = self.session.get(topic['url'])
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract posts using multiple strategies
            post_elements = self._find_post_elements(soup)
            
            for i, post_elem in enumerate(post_elements):
                try:
                    post = self._extract_post_from_html(post_elem, topic, i + 1)
                    if post:
                        posts.append(post)
                except Exception as e:
                    logger.debug(f"Error extracting post {i}: {e}")
                    continue
            
            return posts
            
        except Exception as e:
            logger.error(f"Failed to scrape topic HTML for {topic.get('url', 'unknown')}: {e}")
            return []
