#!/usr/bin/env python3
"""
Enhanced Discourse Forum Scraper with comprehensive error handling and testing capabilities.
"""

import argparse
import json
import csv
import requests
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Any
import logging
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discourse_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Post:
    """Data class representing a forum post"""
    id: int
    topic_id: int
    username: str
    name: str
    created_at: str
    raw: str
    cooked: str
    reply_count: int
    like_count: int
    topic_title: str
    category_name: str
    category_id: int
    trust_level: int
    user_id: int
    post_number: int
    reply_to_post_number: Optional[int] = None
    user_title: Optional[str] = None
    moderator: bool = False
    admin: bool = False
    staff: bool = False
    primary_group_name: Optional[str] = None
    flair_name: Optional[str] = None
    avatar_template: Optional[str] = None

@dataclass
class Topic:
    """Data class representing a forum topic"""
    id: int
    title: str
    category_id: int
    category_name: str
    created_at: str
    last_posted_at: str
    posts_count: int
    reply_count: int
    like_count: int
    views: int
    author_username: str
    author_name: str
    author_id: int
    tags: List[str]
    closed: bool = False
    archived: bool = False
    pinned: bool = False
    visible: bool = True

class DiscourseAPI:
    """Enhanced Discourse API client with comprehensive error handling"""
    
    def __init__(self, base_url: str, api_key: str = None, api_username: str = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_username = api_username
        self.session = requests.Session()
        
        # Set up authentication headers if provided
        if api_key and api_username:
            self.session.headers.update({
                'Api-Key': api_key,
                'Api-Username': api_username,
                'Content-Type': 'application/json'
            })
        
        # Test connection
        self._test_connection()
    
    def _test_connection(self):
        """Test the connection to the Discourse instance"""
        try:
            response = self._make_request('GET', '/site.json')
            logger.info(f"Successfully connected to Discourse at {self.base_url}")
            logger.info(f"Site title: {response.get('title', 'Unknown')}")
        except Exception as e:
            logger.error(f"Failed to connect to Discourse: {e}")
            raise
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict:
        """Make a request to the Discourse API with error handling"""
        url = urljoin(self.base_url, endpoint)
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Making {method} request to {url}")
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting
                if response.status_code == 429:
                    wait_time = 60 * (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception(f"Failed to make request after {max_retries} attempts")
    
    def get_categories(self) -> List[Dict]:
        """Get all categories"""
        logger.info("Fetching categories...")
        data = self._make_request('GET', '/categories.json')
        categories = data.get('category_list', {}).get('categories', [])
        logger.info(f"Found {len(categories)} categories")
        return categories
    
    def get_topics(self, category_id: int = None, page: int = 0) -> List[Dict]:
        """Get topics from a category or all topics"""
        if category_id:
            endpoint = f'/c/{category_id}.json'
            logger.info(f"Fetching topics from category {category_id}, page {page}")
        else:
            endpoint = '/latest.json'
            logger.info(f"Fetching latest topics, page {page}")
        
        params = {'page': page} if page > 0 else {}
        data = self._make_request('GET', endpoint, params=params)
        
        topics = data.get('topic_list', {}).get('topics', [])
        logger.info(f"Found {len(topics)} topics on page {page}")
        return topics
    
    def get_topic_posts(self, topic_id: int) -> Dict:
        """Get all posts from a topic"""
        logger.debug(f"Fetching posts for topic {topic_id}")
        endpoint = f'/t/{topic_id}.json'
        return self._make_request('GET', endpoint)
    
    def search_posts(self, query: str, page: int = 0) -> Dict:
        """Search posts using Discourse search"""
        logger.info(f"Searching for: {query}")
        params = {
            'q': query,
            'page': page + 1  # Discourse uses 1-based indexing
        }
        return self._make_request('GET', '/search.json', params=params)

class DiscourseScraper:
    """Main scraper class with enhanced functionality"""
    
    def __init__(self, base_url: str, api_key: str = None, api_username: str = None):
        self.api = DiscourseAPI(base_url, api_key, api_username)
        self.posts = []
        self.topics = []
        self.users_cache = {}
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse Discourse date string to datetime object"""
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            logger.warning(f"Could not parse date: {date_str}")
            return datetime.now()
    
    def _get_user_info(self, user_id: int) -> Dict:
        """Get user information (with caching)"""
        if user_id in self.users_cache:
            return self.users_cache[user_id]
        
        try:
            user_data = self.api._make_request('GET', f'/users/{user_id}.json')
            user_info = user_data.get('user', {})
            self.users_cache[user_id] = user_info
            return user_info
        except Exception as e:
            logger.warning(f"Could not fetch user {user_id}: {e}")
            return {}
    
    def _extract_post_data(self, post_data: Dict, topic_data: Dict) -> Post:
        """Extract and structure post data"""
        # Get user information
        user_info = {}
        if 'user_id' in post_data:
            user_info = self._get_user_info(post_data['user_id'])
        
        return Post(
            id=post_data.get('id', 0),
            topic_id=topic_data.get('id', 0),
            username=post_data.get('username', ''),
            name=post_data.get('name', ''),
            created_at=post_data.get('created_at', ''),
            raw=post_data.get('raw', ''),
            cooked=post_data.get('cooked', ''),
            reply_count=post_data.get('reply_count', 0),
            like_count=post_data.get('like_count', 0),
            topic_title=topic_data.get('title', ''),
            category_name=topic_data.get('category_name', ''),
            category_id=topic_data.get('category_id', 0),
            trust_level=post_data.get('trust_level', 0),
            user_id=post_data.get('user_id', 0),
            post_number=post_data.get('post_number', 0),
            reply_to_post_number=post_data.get('reply_to_post_number'),
            user_title=user_info.get('title'),
            moderator=post_data.get('moderator', False),
            admin=post_data.get('admin', False),
            staff=post_data.get('staff', False),
            primary_group_name=user_info.get('primary_group_name'),
            flair_name=user_info.get('flair_name'),
            avatar_template=post_data.get('avatar_template')
        )
    
    def _extract_topic_data(self, topic_data: Dict) -> Topic:
        """Extract and structure topic data"""
        return Topic(
            id=topic_data.get('id', 0),
            title=topic_data.get('title', ''),
            category_id=topic_data.get('category_id', 0),
            category_name=topic_data.get('category_name', ''),
            created_at=topic_data.get('created_at', ''),
            last_posted_at=topic_data.get('last_posted_at', ''),
            posts_count=topic_data.get('posts_count', 0),
            reply_count=topic_data.get('reply_count', 0),
            like_count=topic_data.get('like_count', 0),
            views=topic_data.get('views', 0),
            author_username=topic_data.get('last_poster_username', ''),
            author_name=topic_data.get('last_poster_name', ''),
            author_id=topic_data.get('posters', [{}])[0].get('user_id', 0) if topic_data.get('posters') else 0,
            tags=topic_data.get('tags', []),
            closed=topic_data.get('closed', False),
            archived=topic_data.get('archived', False),
            pinned=topic_data.get('pinned', False),
            visible=topic_data.get('visible', True)
        )
    
    def scrape_by_date_range(self, start_date: datetime, end_date: datetime, 
                           category_ids: List[int] = None) -> None:
        """Scrape posts within a date range"""
        logger.info(f"Scraping posts from {start_date} to {end_date}")
        
        # Get categories to scrape
        if category_ids is None:
            categories = self.api.get_categories()
            category_ids = [cat['id'] for cat in categories]
        
        total_posts = 0
        total_topics = 0
        
        for category_id in category_ids:
            logger.info(f"Processing category {category_id}")
            page = 0
            
            while True:
                try:
                    topics = self.api.get_topics(category_id, page)
                    if not topics:
                        break
                    
                    category_posts = 0
                    for topic_data in topics:
                        # Check if topic is within date range
                        topic_date = self._parse_date(topic_data.get('created_at', ''))
                        if not (start_date <= topic_date <= end_date):
                            continue
                        
                        # Extract topic data
                        topic = self._extract_topic_data(topic_data)
                        self.topics.append(topic)
                        total_topics += 1
                        
                        # Get posts for this topic
                        try:
                            topic_posts_data = self.api.get_topic_posts(topic_data['id'])
                            posts = topic_posts_data.get('post_stream', {}).get('posts', [])
                            
                            for post_data in posts:
                                post_date = self._parse_date(post_data.get('created_at', ''))
                                if start_date <= post_date <= end_date:
                                    post = self._extract_post_data(post_data, topic_data)
                                    self.posts.append(post)
                                    category_posts += 1
                                    total_posts += 1
                            
                            # Add delay to respect rate limits
                            time.sleep(0.5)
                            
                        except Exception as e:
                            logger.error(f"Error processing topic {topic_data['id']}: {e}")
                            continue
                    
                    logger.info(f"Category {category_id}, page {page}: {category_posts} posts")
                    
                    # Break if no more pages
                    if len(topics) < 30:  # Discourse typically returns 30 topics per page
                        break
                    
                    page += 1
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error processing category {category_id}, page {page}: {e}")
                    break
        
        logger.info(f"Scraping complete: {total_topics} topics, {total_posts} posts")
    
    def test_api_connection(self) -> Dict[str, Any]:
        """Test API connection and return diagnostic information"""
        logger.info("Testing API connection...")
        
        test_results = {
            'connection_status': 'unknown',
            'site_info': {},
            'categories_count': 0,
            'latest_topics_count': 0,
            'api_authenticated': False,
            'errors': []
        }
        
        try:
            # Test basic connection
            site_data = self.api._make_request('GET', '/site.json')
            test_results['site_info'] = {
                'title': site_data.get('title', 'Unknown'),
                'description': site_data.get('description', ''),
                'locale': site_data.get('locale', 'en')
            }
            test_results['connection_status'] = 'success'
            
            # Test categories
            categories = self.api.get_categories()
            test_results['categories_count'] = len(categories)
            
            # Test latest topics
            latest_topics = self.api.get_topics()
            test_results['latest_topics_count'] = len(latest_topics)
            
            # Test if API is authenticated
            if self.api.api_key and self.api.api_username:
                test_results['api_authenticated'] = True
            
            logger.info("API connection test successful")
            
        except Exception as e:
            test_results['connection_status'] = 'failed'
            test_results['errors'].append(str(e))
            logger.error(f"API connection test failed: {e}")
        
        return test_results
    
    def save_data(self, format_type: str, filename: str = None) -> str:
        """Save scraped data to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format_type.lower() == 'json':
            filename = filename or f"discourse_data_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'topics': [asdict(topic) for topic in self.topics],
                    'posts': [asdict(post) for post in self.posts],
                    'metadata': {
                        'scraped_at': datetime.now().isoformat(),
                        'total_topics': len(self.topics),
                        'total_posts': len(self.posts)
                    }
                }, f, indent=2, ensure_ascii=False)
        
        elif format_type.lower() == 'csv':
            # Save posts to CSV
            posts_filename = filename or f"discourse_posts_{timestamp}.csv"
            with open(posts_filename, 'w', newline='', encoding='utf-8') as f:
                if self.posts:
                    writer = csv.DictWriter(f, fieldnames=asdict(self.posts[0]).keys())
                    writer.writeheader()
                    for post in self.posts:
                        writer.writerow(asdict(post))
            
            # Save topics to CSV
            topics_filename = f"discourse_topics_{timestamp}.csv"
            with open(topics_filename, 'w', newline='', encoding='utf-8') as f:
                if self.topics:
                    writer = csv.DictWriter(f, fieldnames=asdict(self.topics[0]).keys())
                    writer.writeheader()
                    for topic in self.topics:
                        writer.writerow(asdict(topic))
            
            filename = posts_filename
        
        logger.info(f"Data saved to {filename}")
        return filename
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about scraped data"""
        if not self.posts:
            return {'error': 'No data scraped yet'}
        
        # Calculate statistics
        stats = {
            'total_posts': len(self.posts),
            'total_topics': len(self.topics),
            'unique_users': len(set(post.username for post in self.posts)),
            'date_range': {
                'earliest': min(post.created_at for post in self.posts),
                'latest': max(post.created_at for post in self.posts)
            },
            'categories': {},
            'top_users': {},
            'avg_posts_per_topic': len(self.posts) / len(self.topics) if self.topics else 0
        }
        
        # Category statistics
        for post in self.posts:
            cat_name = post.category_name or 'Unknown'
            if cat_name not in stats['categories']:
                stats['categories'][cat_name] = 0
            stats['categories'][cat_name] += 1
        
        # Top users by post count
        user_posts = {}
        for post in self.posts:
            if post.username not in user_posts:
                user_posts[post.username] = 0
            user_posts[post.username] += 1
        
        stats['top_users'] = dict(sorted(user_posts.items(), 
                                       key=lambda x: x[1], reverse=True)[:10])
        
        return stats

def main():
    parser = argparse.ArgumentParser(
        description='Enhanced Discourse Forum Scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python discourse_scraper.py --test
  python discourse_scraper.py --start-date 2024-01-01 --end-date 2024-01-31
  python discourse_scraper.py --format csv --stats
        """
    )
    
    parser.add_argument('--base-url', 
                       default='https://discourse.onlinedegree.iitm.ac.in',
                       help='Base URL of the Discourse instance')
    
    parser.add_argument('--api-key',
                       help='Discourse API key (optional, for authenticated requests)')
    
    parser.add_argument('--api-username',
                       help='Discourse API username (required if using API key)')
    
    parser.add_argument('--start-date',
                       type=str,
                       help='Start date (YYYY-MM-DD format)')
    
    parser.add_argument('--end-date',
                       type=str,
                       help='End date (YYYY-MM-DD format)')
    
    parser.add_argument('--format',
                       choices=['json', 'csv'],
                       default='json',
                       help='Output format (default: json)')
    
    parser.add_argument('--output',
                       help='Output filename')
    
    parser.add_argument('--stats',
                       action='store_true',
                       help='Display statistics after scraping')
    
    parser.add_argument('--test',
                       action='store_true',
                       help='Test API connection and display diagnostic information')
    
    parser.add_argument('--categories',
                       nargs='+',
                       type=int,
                       help='Specific category IDs to scrape (default: all)')
    
    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize scraper
        scraper = DiscourseScraper(
            base_url=args.base_url,
            api_key=args.api_key,
            api_username=args.api_username
        )
        
        # Test mode
        if args.test:
            logger.info("Running in test mode...")
            test_results = scraper.test_api_connection()
            
            print("\n" + "="*50)
            print("DISCOURSE API CONNECTION TEST")
            print("="*50)
            print(f"Connection Status: {test_results['connection_status'].upper()}")
            
            if test_results['site_info']:
                print(f"Site Title: {test_results['site_info']['title']}")
                print(f"Site Description: {test_results['site_info']['description']}")
                print(f"Locale: {test_results['site_info']['locale']}")
            
            print(f"Categories Available: {test_results['categories_count']}")
            print(f"Latest Topics: {test_results['latest_topics_count']}")
            print(f"API Authenticated: {test_results['api_authenticated']}")
            
            if test_results['errors']:
                print("\nErrors:")
                for error in test_results['errors']:
                    print(f"  - {error}")
            
            print("="*50)
            return
        
        # Parse dates
        if args.start_date and args.end_date:
            try:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
            except ValueError as e:
                logger.error(f"Invalid date format: {e}")
                logger.error("Please use YYYY-MM-DD format")
                return
        else:
            # Default to last 30 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            logger.info(f"Using default date range: {start_date.date()} to {end_date.date()}")
        
        # Scrape data
        scraper.scrape_by_date_range(
            start_date=start_date,
            end_date=end_date,
            category_ids=args.categories
        )
        
        # Save data
        if scraper.posts or scraper.topics:
            filename = scraper.save_data(args.format, args.output)
            logger.info(f"Data saved to {filename}")
        else:
            logger.warning("No data found to save")
        
        # Display statistics
        if args.stats:
            stats = scraper.get_stats()
            print("\n" + "="*50)
            print("SCRAPING STATISTICS")
            print("="*50)
            
            if 'error' in stats:
                print(f"Error: {stats['error']}")
            else:
                print(f"Total Posts: {stats['total_posts']}")
                print(f"Total Topics: {stats['total_topics']}")
                print(f"Unique Users: {stats['unique_users']}")
                print(f"Average Posts per Topic: {stats['avg_posts_per_topic']:.2f}")
                
                if stats['date_range']:
                    print(f"Date Range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
                
                if stats['categories']:
                    print("\nPosts by Category:")
                    for cat, count in sorted(stats['categories'].items(), 
                                           key=lambda x: x[1], reverse=True):
                        print(f"  {cat}: {count}")
                
                if stats['top_users']:
                    print("\nTop 10 Users by Post Count:")
                    for user, count in list(stats['top_users'].items())[:10]:
                        print(f"  {user}: {count}")
            
            print("="*50)
    
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise

if __name__ == "__main__":
    main()
