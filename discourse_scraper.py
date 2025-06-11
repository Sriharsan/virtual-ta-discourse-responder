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

class DiscourseScraper:
    def __init__(self, base_url="https://discourse.onlinedegree.iitm.ac.in"):
        self.base_url = base_url
        self.headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/115.0.0.0 Safari/537.36')
        }
        
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
        
        # TDS Knowledge Base category ID is 34
        category_url = f"{self.base_url}/c/courses/tds-kb/34.json"
        
        try:
            # Parse date filters
            start_dt = None
            end_dt = None
            if start_date:
                start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
            if end_date:
                end_dt = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
            
            print(f"Scraping TDS posts from {start_date or 'beginning'} to {end_date or 'now'}...")
            
            # Get topics from category
            response = requests.get(category_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            topics = data.get('topic_list', {}).get('topics', [])
            
            print(f"Found {len(topics)} topics in TDS category")
            
            for topic in topics:
                try:
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
                    
                    topic_response = requests.get(topic_url, headers=self.headers, timeout=10)
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
                                'like_count': post.get('actions_summary', [{}])[0].get('count', 0) if post.get('actions_summary') else 0,
                                'topic_views': topic.get('views', 0),
                                'topic_posts_count': topic.get('posts_count', 0)
                            }
                            posts.append(post_info)
                    
                    # Rate limiting
                    import time
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error processing topic {topic.get('id', 'unknown')}: {e}")
                    continue
            
            print(f"Successfully scraped {len(posts)} posts")
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if output_format.lower() == 'csv':
                filename = f"tds_posts_{timestamp}.csv"
                self.save_to_csv(posts, filename)
            else:
                filename = f"tds_posts_{timestamp}.json"
                self.save_to_json(posts, filename)
            
            print(f"Data saved to {filename}")
            return posts
            
        except Exception as e:
            print(f"Error scraping Discourse: {e}")
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
    
    args = parser.parse_args()
    
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
    
    # Create scraper and run
    scraper = DiscourseScraper()
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
