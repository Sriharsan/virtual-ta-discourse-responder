import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TDSScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def scrape_tds_course_site(self):
        """Scrape the main TDS course website"""
        try:
            url = "https://tds.s-anand.net/#/2025-01/"
            
            # Since this is a single-page application, we might need to handle JavaScript
            # For now, let's try direct scraping
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            course_data = {
                'url': url,
                'title': soup.title.string if soup.title else 'TDS Course',
                'scraped_at': datetime.now().isoformat(),
                'content_type': 'course_website'
            }
            
            # Look for specific content sections
            # Since it's an SPA, we might find content in script tags or data attributes
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and ('course' in script.string.lower() or 'tds' in script.string.lower()):
                    # Try to extract relevant information
                    course_data['script_content'] = script.string[:500]  # First 500 chars
                    break
            
            # Look for any visible text content
            text_content = soup.get_text()
            if text_content:
                course_data['text_preview'] = text_content[:1000]  # First 1000 chars
            
            logger.info(f"Scraped TDS course site: {len(text_content)} characters")
            return course_data
            
        except Exception as e:
            logger.error(f"Error scraping TDS course site: {e}")
            return None
    
    def scrape_discourse_category(self):
        """Scrape the Discourse TDS category"""
        try:
            base_url = "https://discourse.onlinedegree.iitm.ac.in"
            category_id = 34  # TDS KB category
            
            # Try to get category data via JSON API
            json_url = f"{base_url}/c/courses/tds-kb/{category_id}.json"
            
            response = self.session.get(json_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                topics = data.get('topic_list', {}).get('topics', [])
                
                discourse_data = {
                    'url': f"{base_url}/c/courses/tds-kb/{category_id}",
                    'scraped_at': datetime.now().isoformat(),
                    'content_type': 'discourse_forum',
                    'topics_count': len(topics),
                    'topics': []
                }
                
                for topic in topics:
                    topic_data = {
                        'id': topic.get('id'),
                        'title': topic.get('title', ''),
                        'slug': topic.get('slug', ''),
                        'posts_count': topic.get('posts_count', 0),
                        'reply_count': topic.get('reply_count', 0),
                        'views': topic.get('views', 0),
                        'last_posted_at': topic.get('last_posted_at'),
                        'created_at': topic.get('created_at'),
                        'url': f"{base_url}/t/{topic.get('slug', '')}/{topic.get('id', '')}"
                    }
                    discourse_data['topics'].append(topic_data)
                
                logger.info(f"Scraped {len(topics)} topics from Discourse")
                return discourse_data
            
            else:
                # Fallback: scrape the HTML page
                html_url = f"{base_url}/c/courses/tds-kb/{category_id}"
                response = self.session.get(html_url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                discourse_data = {
                    'url': html_url,
                    'scraped_at': datetime.now().isoformat(),
                    'content_type': 'discourse_forum_html',
                    'title': soup.title.string if soup.title else 'TDS Knowledge Base'
                }
                
                # Look for topic links
                topic_links = soup.find_all('a', href=True)
                topics = []
                
                for link in topic_links:
                    href = link.get('href', '')
                    if '/t/' in href and link.get_text().strip():
                        topics.append({
                            'title': link.get_text().strip(),
                            'url': base_url + href if href.startswith('/') else href
                        })
                
                discourse_data['topics'] = topics[:20]  # Limit to first 20
                discourse_data['topics_count'] = len(topics)
                
                logger.info(f"Scraped {len(topics)} topic links from Discourse HTML")
                return discourse_data
                
        except Exception as e:
            logger.error(f"Error scraping Discourse forum: {e}")
            return None
    
    def scrape_discourse_topic(self, topic_url):
        """Scrape a specific Discourse topic"""
        try:
            response = self.session.get(topic_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            topic_data = {
                'url': topic_url,
                'scraped_at': datetime.now().isoformat(),
                'title': soup.title.string if soup.title else '',
                'content_type': 'discourse_topic'
            }
            
            # Extract posts
            posts = soup.find_all('div', {'class': 'post'})
            post_contents = []
            
            for post in posts[:5]:  # Limit to first 5 posts
                post_content = post.get_text().strip()
                if post_content:
                    post_contents.append(post_content[:500])  # First 500 chars
            
            topic_data['posts'] = post_contents
            topic_data['posts_count'] = len(post_contents)
            
            logger.info(f"Scraped topic: {topic_data['title']}")
            return topic_data
            
        except Exception as e:
            logger.error(f"Error scraping topic {topic_url}: {e}")
            return None
    
    def save_scraped_data(self, data, filename):
        """Save scraped data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved data to {filename}")
        except Exception as e:
            logger.error(f"Error saving data to {filename}: {e}")
    
    def scrape_all(self):
        """Scrape all TDS-related data"""
        all_data = {
            'scraped_at': datetime.now().isoformat(),
            'sources': []
        }
        
        # Scrape TDS course website
        logger.info("Scraping TDS course website...")
        tds_data = self.scrape_tds_course_site()
        if tds_data:
            all_data['sources'].append(tds_data)
        
        time.sleep(2)  # Be respectful with requests
        
        # Scrape Discourse forum
        logger.info("Scraping Discourse forum...")
        discourse_data = self.scrape_discourse_category()
        if discourse_data:
            all_data['sources'].append(discourse_data)
            
            # Scrape a few specific topics for more detailed content
            if discourse_data.get('topics'):
                logger.info("Scraping specific topics...")
                for topic in discourse_data['topics'][:3]:  # Scrape first 3 topics
                    topic_url = topic.get('url')
                    if topic_url:
                        time.sleep(1)  # Rate limiting
                        topic_data = self.scrape_discourse_topic(topic_url)
                        if topic_data:
                            all_data['sources'].append(topic_data)
        
        return all_data

def main():
    """Main function to run the scraper"""
    scraper = TDSScraper()
    
    logger.info("Starting TDS data scraping...")
    scraped_data = scraper.scrape_all()
    
    # Save to file
    filename = f"tds_scraped_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    scraper.save_scraped_data(scraped_data, filename)
    
    # Print summary
    print(f"\nScraping completed!")
    print(f"Total sources scraped: {len(scraped_data['sources'])}")
    print(f"Data saved to: {filename}")
    
    for source in scraped_data['sources']:
        print(f"- {source['content_type']}: {source['url']}")
    
    return scraped_data

if __name__ == "__main__":
    main()
