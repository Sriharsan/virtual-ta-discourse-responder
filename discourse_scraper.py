#!/usr/bin/env python3
"""
Discourse Scraper Script for TDS Course
Scrapes posts from Discourse TDS forum within specified date range
Usage: python discourse_scraper.py --start-date 2025-01-01 --end-date 2025-02-01
"""

import requests
import json
import argparse
from datetime import datetime, timezone
import csv
import os
from urllib.parse import urljoin
import time

class DiscourseScraper:
    def __init__(self, base_url="https://discourse.onlinedegree.iitm.ac.in"):
        self.base_url = base_url
        self.session = requests.Session()
        # Enhanced headers to avoid being blocked
        self.headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/120.0.0.0 Safari/537.36'),
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': f'{base_url}/',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.session.headers.update(self.headers)
        
    def test_access(self):
        """Test different endpoints to find accessible ones"""
        test_urls = [
            f"{self.base_url}/latest.json",
            f"{self.base_url}/categories.json",
            f"{self.base_url}/c/courses/tds-kb/34.json",
            f"{self.base_url}/c/courses/tds-kb/34/l/latest.json"
        ]
        
        print("Testing endpoint access...")
        for url in test_urls:
            try:
                response = self.session.get(url, timeout=10)
                print(f"✓ {url}: {response.status_code}")
                if response.status_code == 200:
                    return url
            except Exception as e:
                print(f"✗ {url}: {e}")
        
        return None
        
    def scrape_tds_posts(self, start_date=None, end_date=None, output_format='json'):
        """
        Scrape TDS course posts within date range
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format  
            output_format (str): Output format - 'json' or 'csv'
        
        Returns:
            list: List of scraped posts
        """
        posts = []
        
        # Parse date filters
        start_dt = None
        end_dt = None
        if start_date:
            start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        if end_date:
            end_dt = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
        
        print(f"Scraping TDS posts from {start_date or 'beginning'} to {end_date or 'now'}...")
        
        # Try multiple URL patterns
        urls_to_try = [
            f"{self.base_url}/c/courses/tds-kb/34/l/latest.json",
            f"{self.base_url}/c/courses/tds-kb/34.json",
            f"{self.base_url}/latest.json?category=34",
            f"{self.base_url}/categories.json"
        ]
        
        topics = []
        successful_url = None
        
        for url in urls_to_try:
            try:
                print(f"Trying URL: {url}")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"✓ Success with {url}")
                    
                    # Extract topics based on response structure
                    if 'topic_list' in data:
                        topics = data['topic_list'].get('topics', [])
                        successful_url = url
                        break
                    elif 'latest' in data:
                        topics = data['latest'].get('topics', [])
                        successful_url = url
                        break
                    elif 'category_list' in data:
                        # Try to find TDS category
                        categories = data['category_list'].get('categories', [])
                        for cat in categories:
                            if 'tds' in cat.get('name', '').lower():
                                print(f"Found TDS category: {cat}")
                        continue
                    else:
                        print(f"Unknown response structure from {url}")
                        continue
                        
                elif response.status_code == 403:
                    print(f"✗ 403 Forbidden: {url}")
                    continue
                else:
                    print(f"✗ HTTP {response.status_code}: {url}")
                    continue
                    
            except requests.exceptions.RequestException as e:
                print(f"✗ Request error for {url}: {e}")
                continue
            except json.JSONDecodeError as e:
                print(f"✗ JSON decode error for {url}: {e}")
                continue
        
        if not topics:
            print("❌ Could not access any Discourse endpoints")
            # Try fallback method - scraping from RSS or sitemap
            return self.fallback_scraping(start_date, end_date, output_format)
        
        print(f"Found {len(topics)} topics using {successful_url}")
        
        # Filter topics by TDS relevance if we got all topics
        if 'latest.json' in successful_url or 'categories.json' in successful_url:
            tds_topics = []
            for topic in topics:
                title = topic.get('title', '').lower()
                category_id = topic.get('category_id')
                if 'tds' in title or category_id == 34:
                    tds_topics.append(topic)
            topics = tds_topics
            print(f"Filtered to {len(topics)} TDS-related topics")
        
        # Process each topic
        for i, topic in enumerate(topics):
            try:
                print(f"Processing topic {i+1}/{len(topics)}: {topic.get('title', 'Untitled')}")
                
                # Parse topic date
                last_posted_str = topic.get('last_posted_at', '')
                if last_posted_str:
                    topic_date = datetime.fromisoformat(last_posted_str.replace('Z', '+00:00'))
                    
                    # Filter by date range
                    if start_dt and topic_date < start_dt:
                        continue
                    if end_dt and topic_date > end_dt:
                        continue
                
                # Get detailed topic information
                topic_id = topic.get('id')
                topic_url = f"{self.base_url}/t/{topic_id}.json"
                
                time.sleep(0.5)  # Rate limiting
                
                topic_response = self.session.get(topic_url, timeout=15)
                if topic_response.status_code == 200:
                    topic_data = topic_response.json()
                    
                    # Extract posts from topic
                    topic_posts = topic_data.get('post_stream', {}).get('posts', [])
                    
                    for post in topic_posts:
                        post_date_str = post.get('created_at', '')
                        if post_date_str:
                            post_date = datetime.fromisoformat(post_date_str.replace('Z', '+00:00'))
                            
                            # Filter post by date range
                            if start_dt and post_date < start_dt:
                                continue
                            if end_dt and post_date > end_dt:
                                continue
                        
                        post_info = {
                            'topic_id': topic_id,
                            'topic_title': topic.get('title', ''),
                            'topic_url': f"{self.base_url}/t/{topic.get('slug', '')}/{topic_id}",
                            'post_id': post.get('id'),
                            'post_number': post.get('post_number'),
                            'username': post.get('username', ''),
                            'created_at': post_date_str,
                            'updated_at': post.get('updated_at', ''),
                            'raw_content': post.get('raw', ''),
                            'cooked_content': post.get('cooked', ''),
                            'reply_count': post.get('reply_count', 0),
                            'like_count': self.get_like_count(post),
                            'topic_views': topic.get('views', 0),
                            'topic_posts_count': topic.get('posts_count', 0)
                        }
                        posts.append(post_info)
                
                elif topic_response.status_code == 403:
                    print(f"  ✗ 403 Forbidden for topic {topic_id}")
                    continue
                else:
                    print(f"  ✗ HTTP {topic_response.status_code} for topic {topic_id}")
                    continue
                
            except Exception as e:
                print(f"  ✗ Error processing topic {topic.get('id', 'unknown')}: {e}")
                continue
        
        print(f"Successfully scraped {len(posts)} posts")
        
        # Save to file
        if posts:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if output_format.lower() == 'csv':
                filename = f"tds_posts_{timestamp}.csv"
                self.save_to_csv(posts, filename)
            else:
                filename = f"tds_posts_{timestamp}.json"
                self.save_to_json(posts, filename)
            
            print(f"Data saved to {filename}")
        
        return posts
    
    def get_like_count(self, post):
        """Extract like count from post actions"""
        actions = post.get('actions_summary', [])
        for action in actions:
            if action.get('id') == 2:  # Like action ID
                return action.get('count', 0)
        return 0
    
    def fallback_scraping(self, start_date, end_date, output_format):
        """Fallback method when JSON API is not accessible"""
        print("Attempting fallback scraping method...")
        
        # Try RSS feed
        rss_urls = [
            f"{self.base_url}/c/courses/tds-kb/34.rss",
            f"{self.base_url}/latest.rss"
        ]
        
        for rss_url in rss_urls:
            try:
                response = self.session.get(rss_url, timeout=10)
                if response.status_code == 200:
                    print(f"✓ RSS accessible: {rss_url}")
                    # Parse RSS feed (would need feedparser library)
                    print("RSS parsing would require 'feedparser' library")
                    return []
            except Exception as e:
                print(f"✗ RSS failed: {e}")
        
        print("❌ All fallback methods failed")
        return []
    
    def save_to_json(self, posts, filename):
        """Save posts to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(posts, f, indent=2, ensure_ascii=False)
    
    def save_to_csv(self, posts, filename):
        """Save posts to CSV file"""
        if not posts:
            return
            
        fieldnames = posts[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(posts)
    
    def get_statistics(self, posts):
        """Generate statistics from scraped posts"""
        if not posts:
            return {}
        
        stats = {
            'total_posts': len(posts),
            'unique_topics': len(set(post['topic_id'] for post in posts)),
            'unique_users': len(set(post['username'] for post in posts)),
            'date_range': {
                'earliest': min(post['created_at'] for post in posts if post['created_at']),
                'latest': max(post['created_at'] for post in posts if post['created_at'])
            },
            'top_contributors': {},
            'most_active_topics': {}
        }
        
        # Top contributors by post count
        user_counts = {}
        for post in posts:
            user = post['username']
            user_counts[user] = user_counts.get(user, 0) + 1
        
        stats['top_contributors'] = dict(sorted(user_counts.items(), 
                                              key=lambda x: x[1], reverse=True)[:10])
        
        # Most active topics by post count
        topic_counts = {}
        for post in posts:
            topic = post['topic_title']
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        stats['most_active_topics'] = dict(sorted(topic_counts.items(), 
                                                 key=lambda x: x[1], reverse=True)[:10])
        
        return stats

def main():
    parser = argparse.ArgumentParser(description='Scrape TDS Discourse posts within date range')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--format', choices=['json', 'csv'], default='json', 
                       help='Output format (json or csv)')
    parser.add_argument('--stats', action='store_true', 
                       help='Show statistics after scraping')
    parser.add_argument('--test', action='store_true',
                       help='Test endpoint access without scraping')
    
    args = parser.parse_args()
    
    # Create scraper
    scraper = DiscourseScraper()
    
    if args.test:
        scraper.test_access()
        return
    
    # Validate dates
    if args.start_date:
        try:
            datetime.fromisoformat(args.start_date)
        except ValueError:
            print("Error: Invalid start date format. Use YYYY-MM-DD")
            return
    
    if args.end_date:
        try:
            datetime.fromisoformat(args.end_date)
        except ValueError:
            print("Error: Invalid end date format. Use YYYY-MM-DD")
            return
    
    # Run scraper
    posts = scraper.scrape_tds_posts(
        start_date=args.start_date,
        end_date=args.end_date,
        output_format=args.format
    )
    
    if args.stats and posts:
        print("\n=== SCRAPING STATISTICS ===")
        stats = scraper.get_statistics(posts)
        
        print(f"Total posts: {stats['total_posts']}")
        print(f"Unique topics: {stats['unique_topics']}")
        print(f"Unique users: {stats['unique_users']}")
        print(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
        
        print("\nTop contributors:")
        for user, count in list(stats['top_contributors'].items())[:5]:
            print(f"  {user}: {count} posts")
        
        print("\nMost active topics:")
        for topic, count in list(stats['most_active_topics'].items())[:5]:
            print(f"  {topic}: {count} posts")

if __name__ == "__main__":
    main()
