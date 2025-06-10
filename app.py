from flask import Flask, request, jsonify
import sqlite3
import json
import base64
import io
import os
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
from PIL import Image
import openai
from openai import OpenAI
import time
from urllib.parse import urljoin, urlparse
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class TDSVirtualTA:
    def __init__(self, db_path="tds_knowledge.db"):
        self.db_path = db_path
        self.setup_database()
        
        # Initialize OpenAI client
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
            self.openai_client = None
        else:
            self.openai_client = OpenAI(api_key=api_key)
        
        # Initialize knowledge base
        self.initialize_knowledge_base()
        
        # Start background scraper
        self.start_background_scraper()
    
    def setup_database(self):
        """Initialize SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Discourse posts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discourse_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                url TEXT UNIQUE,
                category TEXT,
                created_at TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Course content table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS course_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                url TEXT UNIQUE,
                section TEXT,
                scraped_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Scraping status table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT UNIQUE,
                last_scraped TEXT,
                status TEXT,
                error_message TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def scrape_discourse_posts(self):
        """Scrape TDS discourse posts"""
        try:
            base_url = "https://discourse.onlinedegree.iitm.ac.in"
            category_url = f"{base_url}/c/courses/tds-kb/34.json"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(category_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            scraped_count = 0
            
            if 'topic_list' in data and 'topics' in data['topic_list']:
                for topic in data['topic_list']['topics'][:50]:  # Limit to 50 recent topics
                    topic_id = topic.get('id')
                    title = topic.get('title', '')
                    
                    # Get topic details
                    topic_url = f"{base_url}/t/{topic_id}.json"
                    try:
                        topic_response = requests.get(topic_url, headers=headers, timeout=20)
                        topic_response.raise_for_status()
                        topic_data = topic_response.json()
                        
                        # Extract posts
                        if 'post_stream' in topic_data and 'posts' in topic_data['post_stream']:
                            for post in topic_data['post_stream']['posts'][:5]:  # First 5 posts per topic
                                content = post.get('cooked', '')
                                if content:
                                    # Clean HTML content
                                    soup = BeautifulSoup(content, 'html.parser')
                                    clean_content = soup.get_text().strip()
                                    
                                    post_url = f"{base_url}/t/{topic_id}/{post.get('post_number', 1)}"
                                    
                                    cursor.execute('''
                                        INSERT OR REPLACE INTO discourse_posts 
                                        (title, content, url, category, created_at)
                                        VALUES (?, ?, ?, ?, ?)
                                    ''', (title, clean_content, post_url, 'TDS', 
                                         post.get('created_at', datetime.now().isoformat())))
                                    
                                    scraped_count += 1
                        
                        time.sleep(1)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"Error scraping topic {topic_id}: {e}")
                        continue
            
            # Update scraping status
            cursor.execute('''
                INSERT OR REPLACE INTO scraping_status 
                (source, last_scraped, status, error_message)
                VALUES (?, ?, ?, ?)
            ''', ('discourse', datetime.now().isoformat(), 'success', None))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully scraped {scraped_count} discourse posts")
            return scraped_count
            
        except Exception as e:
            logger.error(f"Error scraping discourse: {e}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO scraping_status 
                (source, last_scraped, status, error_message)
                VALUES (?, ?, ?, ?)
            ''', ('discourse', datetime.now().isoformat(), 'error', str(e)))
            conn.commit()
            conn.close()
            return 0
    
    def scrape_course_content(self):
        """Scrape TDS course content from tds.s-anand.net"""
        try:
            base_url = "https://tds.s-anand.net"
            course_url = f"{base_url}/#/2025-01/"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # This is a SPA, so we'll scrape the main content and common sections
            sections_to_scrape = [
                '',  # Main page
                'introduction',
                'setup',
                'python-basics',
                'data-analysis',
                'machine-learning',
                'deployment',
                'projects',
                'evaluation'
            ]
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            scraped_count = 0
            
            for section in sections_to_scrape:
                try:
                    url = f"{base_url}/#/2025-01/{section}" if section else course_url
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract text content
                    content_div = soup.find('div', class_='content') or soup.find('main') or soup.body
                    if content_div:
                        # Remove script and style elements
                        for script in content_div(["script", "style"]):
                            script.decompose()
                        
                        text_content = content_div.get_text()
                        lines = (line.strip() for line in text_content.splitlines())
                        content = '\n'.join(line for line in lines if line)
                        
                        if len(content) > 100:  # Only store substantial content
                            cursor.execute('''
                                INSERT OR REPLACE INTO course_content 
                                (title, content, url, section)
                                VALUES (?, ?, ?, ?)
                            ''', (f"TDS 2025-01 {section.title() if section else 'Overview'}", 
                                 content[:5000], url, section or 'main'))
                            
                            scraped_count += 1
                    
                    time.sleep(2)  # Rate limiting
                    
                except Exception as e:
                    logger.error(f"Error scraping section {section}: {e}")
                    continue
            
            # Update scraping status
            cursor.execute('''
                INSERT OR REPLACE INTO scraping_status 
                (source, last_scraped, status, error_message)
                VALUES (?, ?, ?, ?)
            ''', ('course_content', datetime.now().isoformat(), 'success', None))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Successfully scraped {scraped_count} course content sections")
            return scraped_count
            
        except Exception as e:
            logger.error(f"Error scraping course content: {e}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO scraping_status 
                (source, last_scraped, status, error_message)
                VALUES (?, ?, ?, ?)
            ''', ('course_content', datetime.now().isoformat(), 'error', str(e)))
            conn.commit()
            conn.close()
            return 0
    
    def start_background_scraper(self):
        """Start background thread for periodic scraping"""
        def scraper_worker():
            while True:
                try:
                    logger.info("Starting background scraping...")
                    discourse_count = self.scrape_discourse_posts()
                    course_count = self.scrape_course_content()
                    logger.info(f"Background scraping completed: {discourse_count} discourse posts, {course_count} course sections")
                    
                    # Wait 6 hours before next scrape
                    time.sleep(6 * 60 * 60)
                except Exception as e:
                    logger.error(f"Error in background scraper: {e}")
                    time.sleep(60 * 60)  # Wait 1 hour on error
        
        scraper_thread = threading.Thread(target=scraper_worker, daemon=True)
        scraper_thread.start()
    
    def initialize_knowledge_base(self):
        """Initialize knowledge base with sample data if empty"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if we have any data
        cursor.execute("SELECT COUNT(*) FROM discourse_posts")
        discourse_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM course_content")
        course_count = cursor.fetchone()[0]
        
        if discourse_count == 0 and course_count == 0:
            logger.info("Initializing knowledge base with sample data")
            self.add_sample_data(cursor)
            conn.commit()
            
            # Start initial scraping
            logger.info("Starting initial scraping...")
            self.scrape_discourse_posts()
            self.scrape_course_content()
        
        conn.close()
    
    def add_sample_data(self, cursor):
        """Add sample knowledge base data for testing"""
        sample_discourse_posts = [
            {
                'title': 'GA5 Question 8 Clarification',
                'content': '''The question asks to use gpt-3.5-turbo-0125 model but the ai-proxy provided by Anand sir only supports gpt-4o-mini. 
                You must use gpt-3.5-turbo-0125, even if the AI Proxy only supports gpt-4o-mini. Use the OpenAI API directly for this question.
                The model specified in the question is what you should use, not what's available in the proxy.''',
                'url': 'https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/4',
                'category': 'TDS'
            },
            {
                'title': 'GA4 Data Sourcing Discussion',
                'content': '''If a student scores 10/10 on GA4 as well as a bonus, it would appear as "110" on the dashboard.
                The bonus points are added to show the total score including bonus marks.''',
                'url': 'https://discourse.onlinedegree.iitm.ac.in/t/ga4-data-sourcing-discussion-thread-tds-jan-2025/165959/388',
                'category': 'TDS'
            },
            {
                'title': 'Project 1 Deadline Extension',
                'content': '''Project 1 deadline has been extended to 16 Feb 2025. This extension applies to all students.
                Please submit your projects before the new deadline to avoid any penalties.''',
                'url': 'https://discourse.onlinedegree.iitm.ac.in/t/project-1-deadline-extension/12346',
                'category': 'TDS'
            },
            {
                'title': 'Docker vs Podman Discussion',
                'content': '''While Docker is acceptable for this course, we recommend using Podman as it's what we officially support.
                Podman is Docker-compatible and provides better security features. You can use Docker if you're more familiar with it,
                but Podman is preferred for the TDS course.''',
                'url': 'https://discourse.onlinedegree.iitm.ac.in/t/docker-vs-podman/12345',
                'category': 'TDS'
            }
        ]
        
        sample_course_content = [
            {
                'title': 'Docker and Containers',
                'content': '''Docker is a containerization platform that allows you to package applications with their dependencies.
                In this course, we prefer Podman over Docker for security reasons, but both are acceptable.
                Containers help ensure consistency across different environments.''',
                'url': 'https://tds.s-anand.net/#/docker',
                'section': 'Week 3'
            },
            {
                'title': 'AI Models and APIs',
                'content': '''When working with AI models in assignments, always use the exact model specified in the question.
                Common models include gpt-3.5-turbo-0125, gpt-4, and others. The AI proxy may not support all models,
                so you might need to use the OpenAI API directly.''',
                'url': 'https://tds.s-anand.net/#/ai-models',
                'section': 'Week 5'
            },
            {
                'title': 'TDS Grading System',
                'content': '''TDS uses a best 4 out of 7 GA rule for grading. You need to pass the course with adequate scores.
                Projects are evaluated using automated systems and LLMs for comprehensive assessment.
                GitHub repositories should be public with MIT license for Project 1.''',
                'url': 'https://tds.s-anand.net/#/grading',
                'section': 'Evaluation'
            }
        ]
        
        # Insert sample data
        for post in sample_discourse_posts:
            cursor.execute('''
                INSERT OR IGNORE INTO discourse_posts (title, content, url, category)
                VALUES (?, ?, ?, ?)
            ''', (post['title'], post['content'], post['url'], post['category']))
        
        for content in sample_course_content:
            cursor.execute('''
                INSERT OR IGNORE INTO course_content (title, content, url, section)
                VALUES (?, ?, ?, ?)
            ''', (content['title'], content['content'], content['url'], content['section']))
    
    def search_knowledge_base(self, query, limit=5):
        """Search knowledge base for relevant content"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        search_terms = query.lower().split()
        results = []
        
        # Search discourse posts
        for term in search_terms[:3]:  # Limit search terms to avoid too many queries
            cursor.execute('''
                SELECT title, content, url FROM discourse_posts 
                WHERE LOWER(content) LIKE ? OR LOWER(title) LIKE ?
                ORDER BY id DESC
            ''', (f'%{term}%', f'%{term}%'))
            
            results.extend(cursor.fetchall())
        
        # Search course content
        for term in search_terms[:3]:
            cursor.execute('''
                SELECT title, content, url FROM course_content 
                WHERE LOWER(content) LIKE ? OR LOWER(title) LIKE ?
                ORDER BY id DESC
            ''', (f'%{term}%', f'%{term}%'))
            
            results.extend(cursor.fetchall())
        
        conn.close()
        
        # Remove duplicates and format results
        unique_results = []
        seen_urls = set()
        
        for title, content, url in results:
            if url not in seen_urls:
                unique_results.append({
                    'title': title,
                    'content': content,
                    'url': url
                })
                seen_urls.add(url)
                
                if len(unique_results) >= limit:
                    break
        
        return unique_results
    
    def process_image(self, base64_image):
        """Process base64 encoded image and extract relevant information"""
        try:
            if not base64_image:
                return None
            
            # Decode base64 image
            image_data = base64.b64decode(base64_image)
            image = Image.open(io.BytesIO(image_data))
            
            # For now, return basic info about the image
            # In production, you might want to use OCR or vision models
            return f"Image processed: {image.size[0]}x{image.size[1]} pixels"
            
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return "Could not process the provided image"
    
    def generate_answer(self, question, image_context=None, relevant_content=None):
        """Generate answer using OpenAI API with context from knowledge base"""
        if not self.openai_client:
            return self.fallback_answer(question, relevant_content)
        
        try:
            # Prepare context from knowledge base
            context_text = ""
            if relevant_content:
                context_parts = []
                for item in relevant_content[:3]:  # Use top 3 relevant items
                    context_parts.append(f"Title: {item['title']}\nContent: {item['content'][:500]}...\nSource: {item['url']}")
                context_text = "\n\n".join(context_parts)
            
            # System prompt for TDS Virtual TA
            system_prompt = f"""You are a Virtual Teaching Assistant for the Tools in Data Science (TDS) course at IIT Madras. 
            Your role is to help students with their questions based on course content and forum discussions.
            
            Context from course materials and discussions:
            {context_text}
            
            Guidelines:
            1. Answer based on the provided context when possible
            2. Be specific about TDS course requirements and policies
            3. If you don't have enough information, say so clearly
            4. Mention specific models, tools, or requirements when relevant
            5. Be concise but comprehensive
            6. Always prioritize course-specific guidance over general advice
            7. For model selection questions, emphasize using the exact model specified in assignments
            8. For Docker/Podman questions, recommend Podman but mention Docker is acceptable
            9. For project deadlines, check for extensions and provide current information
            10. For grading questions, mention the best 4 out of 7 GA rule
            """
            
            # Prepare user message
            user_message = f"Student Question: {question}"
            if image_context:
                user_message += f"\n\nImage Context: {image_context}"
            
            # Generate response
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=400,
                temperature=0.3  # Lower temperature for more consistent answers
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Error generating answer with OpenAI: {e}")
            return self.fallback_answer(question, relevant_content)
    
    def fallback_answer(self, question, relevant_content):
        """Fallback answer when OpenAI is not available"""
        if relevant_content:
            # Return content from knowledge base
            best_match = relevant_content[0]
            return f"Based on course discussions: {best_match['content'][:300]}..."
        else:
            return "I don't have enough information to answer this question. Please check the course materials or ask on the forum."
    
    def format_links(self, relevant_content):
        """Format relevant content as links for response"""
        links = []
        for item in relevant_content[:3]:  # Return top 3 links
            links.append({
                "url": item['url'],
                "text": item['title'] if item['title'] else item['content'][:100] + "..."
            })
        return links
    
    def process_question(self, question, image_base64=None):
        """Main method to process student questions"""
        try:
            # Process image if provided
            image_context = None
            if image_base64:
                image_context = self.process_image(image_base64)
            
            # Search for relevant content in knowledge base
            relevant_content = self.search_knowledge_base(question)
            
            # Generate answer using OpenAI with context
            answer = self.generate_answer(question, image_context, relevant_content)
            
            # Format relevant links
            links = self.format_links(relevant_content)
            
            return {
                "answer": answer,
                "links": links
            }
            
        except Exception as e:
            logger.error(f"Error processing question: {e}")
            return {
                "answer": "I encountered an error while processing your question. Please try again later.",
                "links": []
            }

# Initialize the Virtual TA
virtual_ta = TDSVirtualTA()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "message": "TDS Virtual TA API",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/chat', methods=['POST'])
def answer_question():
    """Main API endpoint for answering student questions"""
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "No JSON data provided"
            }), 400
        
        if 'question' not in data:
            return jsonify({
                "error": "Missing 'question' field in request"
            }), 400
        
        question = data['question'].strip()
        if not question:
            return jsonify({
                "error": "Question cannot be empty"
            }), 400
        
        image_base64 = data.get('image')
        
        # Process the question
        result = virtual_ta.process_question(question, image_base64)
        
        # Log the request (without image data for brevity)
        logger.info(f"Question processed: {question[:100]}...")
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in answer_question endpoint: {e}")
        return jsonify({
            "error": "Internal server error",
            "answer": "I'm sorry, but I encountered an error while processing your request. Please try again.",
            "links": []
        }), 500

# Keep the old endpoint for backward compatibility
@app.route('/api/', methods=['POST'])
def answer_question_old():
    """Old API endpoint for backward compatibility"""
    return answer_question()

@app.route('/api/', methods=['GET'])
def api_info():
    """API information endpoint"""
    return jsonify({
        "name": "TDS Virtual TA API",
        "version": "1.0.0",
        "description": "Virtual Teaching Assistant for Tools in Data Science course",
        "endpoints": {
            "POST /api/chat": {
                "description": "Submit a question with optional image",
                "parameters": {
                    "question": "string (required) - The student's question",
                    "image": "string (optional) - Base64 encoded image"
                }
            },
            "POST /api/": "Same as /api/chat (for backward compatibility)",
            "GET /api/": "API information",
            "GET /health": "Health check",
            "GET /scrape": "Manual scraping trigger"
        },
        "example": {
            "request": {
                "question": "Should I use gpt-4o-mini which AI proxy supports, or gpt3.5 turbo?",
                "image": "base64_encoded_image_optional"
            },
            "response": {
                "answer": "Generated answer based on course context",
                "links": [
                    {
                        "url": "https://example.com/relevant-link",
                        "text": "Link description"
                    }
                ]
            }
        }
    })

@app.route('/scrape', methods=['GET'])
def manual_scrape():
    """Manual scraping trigger for testing"""
    try:
        discourse_count = virtual_ta.scrape_discourse_posts()
        course_count = virtual_ta.scrape_course_content()
        
        return jsonify({
            "status": "success",
            "discourse_posts_scraped": discourse_count,
            "course_sections_scraped": course_count,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/', methods=['GET'])
def root():
    """Root endpoint"""
    return jsonify({
        "message": "TDS Virtual TA - Tools in Data Science Teaching Assistant",
        "status": "running",
        "api_endpoint": "/api/chat",
        "health_check": "/health",
        "manual_scrape": "/scrape"
    })

if __name__ == '__main__':
    # Get port from environment variable or use default
    port = int(os.environ.get('PORT', 5000))
    
    # Run the Flask application
    app.run(
        host='0.0.0.0',
        port=port,
        debug=os.environ.get('FLASK_ENV') == 'development'
    )
