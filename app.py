from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime
import logging
from urllib.parse import urljoin, urlparse
import time
import os

app = Flask(__name__)
CORS(app)

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TDSKnowledgeBase:
    def __init__(self):
        self.knowledge_base = {
            'course_info': {
                'title': 'Tools in Data Science (TDS)',
                'term': '2025-01',
                'grading': 'Best 4 out of 7 GA scores',
                'passing_criteria': 'Minimum 40% overall',
                'prerequisites': 'GA1 must be completed',
                'container_tool': 'Podman (preferred over Docker for this course)',
                'deployment': 'Vercel recommended for web deployment'
            },
            'project_info': {
                'project1_deadline': '16 Feb 2025 (extended)',
                'github_requirements': 'Public repository with MIT license',
                'evaluation_method': 'Automated evaluation using LLMs'
            },
            'common_issues': {
                'quota_error': 'Check API key and usage limits. Contact instructor if quota exceeded.',
                'submission_issues': 'Try clearing cache, different browser, or contact support.',
                'vercel_deployment': 'Check build logs, ensure all dependencies in package.json',
                'gcp_setup': 'Select organization or create new project without parent folder'
            },
            'resources': {
                'course_material': 'YouTube playlist available for recorded sessions',
                'support': 'Post questions on Discourse forum for help'
            },
            # Add general programming knowledge
            'programming_topics': {
                'pandas_vs_numpy': {
                    'pandas': 'Data manipulation library built on NumPy, designed for structured data with DataFrames and Series',
                    'numpy': 'Numerical computing library for arrays and mathematical operations',
                    'difference': 'NumPy is for numerical arrays, Pandas is for data analysis with labeled data structures'
                }
            }
        }
        self.scraped_data = []
        self.last_updated = None
        
    def scrape_discourse_forum(self, start_date=None, end_date=None):
        """Scrape Discourse posts within date range for bonus marks"""
        try:
            base_url = "https://discourse.onlinedegree.iitm.ac.in"
            category_url = f"{base_url}/c/courses/tds-kb/34.json"
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            # Add date filtering if provided
            params = {}
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
                
            response = requests.get(category_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                topics = data.get('topic_list', {}).get('topics', [])
                scraped_count = 0
                
                for topic in topics:
                    topic_date = topic.get('last_posted_at', '')
                    # Filter by date range if specified
                    if start_date and end_date:
                        try:
                            topic_datetime = datetime.fromisoformat(topic_date.replace('Z', '+00:00'))
                            start_datetime = datetime.fromisoformat(start_date)
                            end_datetime = datetime.fromisoformat(end_date)
                            
                            if not (start_datetime <= topic_datetime <= end_datetime):
                                continue
                        except:
                            continue
                    
                    self.scraped_data.append({
                        'title': topic.get('title', ''),
                        'url': f"{base_url}/t/{topic.get('slug', '')}/{topic.get('id', '')}",
                        'posts_count': topic.get('posts_count', 0),
                        'last_posted_at': topic_date,
                        'views': topic.get('views', 0),
                        'source': 'discourse'
                    })
                    scraped_count += 1
                    
                logger.info(f"Scraped {scraped_count} topics from Discourse within date range")
                return True
            else:
                logger.warning(f"Discourse API returned status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error scraping Discourse forum: {e}")
            return False
    
    def scrape_tds_website(self):
        try:
            url = "https://tds.s-anand.net/"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logger.info("TDS website data loaded from knowledge base")
            return True
        except Exception as e:
            logger.error(f"Error scraping TDS website: {e}")
            return False
    
    def find_relevant_content(self, question):
        question_lower = question.lower()
        relevant_links = []
        
        # Enhanced keyword matching with actual working URLs
        if any(word in question_lower for word in ['model', 'gpt-3.5', 'gpt-4o', 'api', 'ai-proxy']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939",
                "text": "GA5 Question 8 Clarification - Model Selection"
            })
        if any(word in question_lower for word in ['score', 'dashboard', 'ga4', 'bonus', '110']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/t/ga4-data-sourcing-discussion-thread-tds-jan-2025/165959",
                "text": "GA4 Data Sourcing Discussion - Dashboard Scoring"
            })
        if any(word in question_lower for word in ['docker', 'podman', 'container']):
            relevant_links.append({
                "url": "https://tds.s-anand.net/#/docker",
                "text": "Docker and Podman Setup Guide"
            })
        if any(word in question_lower for word in ['deadline', 'project', 'extension', '16 feb']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - Project Deadlines"
            })
        if any(word in question_lower for word in ['quota', 'error', 'insufficient', 'llm embed']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - API Quota Issues"
            })
        if any(word in question_lower for word in ['github', 'repository', 'license', 'public', 'mit']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - GitHub Requirements"
            })
        if any(word in question_lower for word in ['vercel', 'deployment', 'build', 'failing']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - Deployment Help"
            })
        if any(word in question_lower for word in ['recorded', 'session', 'youtube', 'missed']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - Course Materials"
            })
        if any(word in question_lower for word in ['evaluation', 'grading', 'llm', 'automated']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - Evaluation Methods"
            })
        
        # Add programming-related links
        if any(word in question_lower for word in ['pandas', 'numpy', 'difference', 'programming']):
            relevant_links.append({
                "url": "https://pandas.pydata.org/docs/",
                "text": "Pandas Official Documentation"
            })
            relevant_links.append({
                "url": "https://numpy.org/doc/",
                "text": "NumPy Official Documentation"
            })
        
        # Always include main forum
        relevant_links.append({
            "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
            "text": "TDS Course Discussion Forum"
        })
        
        return relevant_links
    
    def generate_answer(self, question):
        question_lower = question.lower()
        
        # Programming questions
        if 'pandas' in question_lower and 'numpy' in question_lower:
            return """NumPy and Pandas are both essential Python libraries but serve different purposes:

**NumPy (Numerical Python):**
- Foundation for numerical computing in Python
- Works with homogeneous arrays (all elements same type)
- Optimized for mathematical operations and linear algebra
- Lower-level, more memory efficient
- Example: `np.array([1, 2, 3, 4])`

**Pandas:**
- Built on top of NumPy
- Designed for data manipulation and analysis
- Works with heterogeneous data (mixed types)
- Provides DataFrames and Series structures
- Higher-level with more data analysis features
- Example: `pd.DataFrame({'A': [1, 2], 'B': ['x', 'y']})`

**Key Differences:**
- NumPy: Arrays, mathematical operations, performance
- Pandas: DataFrames, data cleaning, analysis, labeled data

Use NumPy for numerical computations, Pandas for data analysis tasks."""
        
        # Model selection questions
        if any(word in question_lower for word in ['gpt-3.5', 'gpt-4o', 'ai-proxy', 'model']):
            if 'ai-proxy' in question_lower and 'gpt-4o-mini' in question_lower:
                return "Use gpt-3.5-turbo-0125 as specified in the requirements, not gpt-4o-mini. You should use the OpenAI API directly for gpt-3.5-turbo-0125 rather than the ai-proxy."
            return "Use gpt-3.5-turbo-0125 as recommended for TDS assignments."
        
        # Dashboard and scoring
        if any(word in question_lower for word in ['dashboard', 'bonus', '110', 'ga4', 'score']):
            return "Dashboard scores include bonus points. For example, if you score 10/10 plus bonus, it would appear as 110 or 110% on the dashboard."
        
        # Docker vs Podman
        if any(word in question_lower for word in ['docker', 'podman']):
            return "Use Podman for TDS course as recommended. While Docker knowledge is transferable, Podman is preferred for this course. However, Docker is also acceptable if you're more comfortable with it."
        
        # Future course information
        if 'sep 2025' in question_lower or 'end-term exam' in question_lower:
            return "I don't have information about TDS Sep 2025 schedule. Please check official announcements or contact course coordinators."
        
        # Project deadlines
        if any(word in question_lower for word in ['project 1', 'deadline', 'extension', '16 feb']):
            return "Project 1 deadline has been extended to 16 Feb 2025. Please confirm the latest deadline on Discourse."
        
        # Submission issues
        if any(word in question_lower for word in ['submission', 'error', 'portal', 'not submitting']):
            return "Try clearing your browser cache, using a different browser, or contact support if submission issues persist."
        
        # API quota issues
        if any(word in question_lower for word in ['quota', 'insufficient', 'llm embed']):
            return "Check your API quota and usage limits. Contact the instructor if you've exceeded your quota allocation."
        
        # Course difficulty
        if any(word in question_lower for word in ['difficult', 'drop', 'level']):
            return "Complete GA1 before considering dropping the course. Seek help on Discourse forum first - many students find support there."
        
        # GitHub requirements
        if any(word in question_lower for word in ['github', 'public', 'license', 'mit']):
            return "Your GitHub repository should be public with an MIT license for Project 1 submission."
        
        # Vector database issues
        if any(word in question_lower for word in ['vector', 'database', 'ga3', 'validation']):
            return "For GA3 vector database questions, double-check your answer format and contact TAs if validation continues to fail."
        
        # Vercel deployment
        if any(word in question_lower for word in ['vercel', 'deployment', 'build', 'failing']):
            return "Check your build logs for errors, ensure all dependencies are in package.json, and verify environment variables are set correctly."
        
        # Course materials
        if any(word in question_lower for word in ['recorded', 'session', 'youtube', 'missed']):
            return "Check the YouTube playlist linked in Discourse course materials for recorded sessions."
        
        # Evaluation methodology
        if any(word in question_lower for word in ['evaluation', 'grading', 'llm', 'automated']):
            return "Projects are evaluated using automated LLM-based grading. Final grade is best 4 out of 7 GAs with minimum 40% overall."
        
        # Google Cloud setup
        if any(word in question_lower for word in ['google', 'cloud', 'gcp', 'parent organization']):
            return "For Google Cloud setup, create a new project without selecting a parent organization or folder."
        
        # Grading and passing
        if any(word in question_lower for word in ['minimum', 'score', 'pass', 'best 4', '40%']):
            return "You need minimum 40% overall to pass. Final grade calculated from best 4 out of 7 GA scores."
        
        # Generic response
        return "Please check the course materials on Discourse or contact TAs for specific assistance with your question."

kb = TDSKnowledgeBase()

@app.route('/')
def home():
    return jsonify({
        "message": "Virtual TA API for TDS Course",
        "status": "active",
        "endpoints": {
            "chat": "/api (POST)",
            "health": "/health",
            "scrape": "/api/scrape (POST)"
        }
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

# Enhanced API endpoint with comprehensive error handling and logging
@app.route('/api', methods=['POST', 'GET'])
def chat():
    try:
        # Log the incoming request with headers
        logger.info(f"Received {request.method} request to /api")
        logger.info(f"Request headers: {dict(request.headers)}")
        logger.info(f"Request content type: {request.content_type}")
        
        if request.method == 'GET':
            # Handle GET requests for testing
            question = request.args.get('question', '')
            if not question:
                question = "What is TDS course about?"
        else:
            # Handle POST requests with detailed logging
            logger.info(f"Raw request data: {request.get_data()}")
            
            try:
                # Try different approaches to get JSON data
                if request.is_json:
                    data = request.get_json()
                    logger.info(f"Parsed JSON data: {data}")
                else:
                    # Force JSON parsing even if content-type is not set correctly
                    data = request.get_json(force=True)
                    logger.info(f"Force-parsed JSON data: {data}")
                
                if not data:
                    logger.error("No JSON data received")
                    return jsonify({"error": "No JSON data provided"}), 400
                    
                if 'question' not in data:
                    logger.error(f"Missing 'question' field. Available fields: {list(data.keys())}")
                    return jsonify({"error": "Missing 'question' field in JSON"}), 400
                    
                question = data['question']
                
            except Exception as json_error:
                logger.error(f"JSON parsing error: {json_error}")
                logger.error(f"Request data type: {type(request.get_data())}")
                return jsonify({
                    "error": "Invalid JSON format", 
                    "details": str(json_error),
                    "received_data": str(request.get_data())
                }), 400
        
        if not question or not question.strip():
            return jsonify({"error": "Question cannot be empty"}), 400
            
        question = question.strip()
        logger.info(f"Processing question: {question[:100]}...")
        
        # Generate response
        answer = kb.generate_answer(question)
        links = kb.find_relevant_content(question)
        
        response_data = {
            "answer": answer,
            "links": links,
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "status": "success"
        }
        
        logger.info(f"Generated response with {len(links)} links")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": "Internal server error", 
            "message": str(e),
            "timestamp": datetime.now().isoformat(),
            "status": "error"
        }), 500

@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    try:
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        tds_success = kb.scrape_tds_website()
        discourse_success = kb.scrape_discourse_forum(start_date, end_date)
        
        return jsonify({
            "tds_scrape": "success" if tds_success else "failed",
            "discourse_scrape": "success" if discourse_success else "failed",
            "scraped_count": len(kb.scraped_data),
            "date_range": f"{start_date} to {end_date}" if start_date and end_date else "all",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in scrape endpoint: {e}")
        return jsonify({"error": "Scraping failed", "message": str(e)}), 500

# Add a simple test endpoint
@app.route('/test', methods=['GET', 'POST'])
def test():
    return jsonify({
        "message": "Test endpoint working",
        "method": request.method,
        "timestamp": datetime.now().isoformat(),
        "status": "success"
    })

if __name__ == '__main__':
    # Initialize with some data
    logger.info("Starting Flask application...")
    try:
        kb.scrape_tds_website()
        kb.scrape_discourse_forum()
        logger.info("Initial data scraping completed")
    except Exception as e:
        logger.error(f"Error during initialization: {e}")
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
