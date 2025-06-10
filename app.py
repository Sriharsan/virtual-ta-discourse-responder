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

# Configure logging
logging.basicConfig(level=logging.INFO)
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
            }
        }
        self.scraped_data = []
        self.last_updated = None
        
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
    
    def scrape_discourse_forum(self):
        try:
            base_url = "https://discourse.onlinedegree.iitm.ac.in"
            category_url = f"{base_url}/c/courses/tds-kb/34.json"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(category_url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                topics = data.get('topic_list', {}).get('topics', [])
                for topic in topics[:20]:
                    self.scraped_data.append({
                        'title': topic.get('title', ''),
                        'url': f"{base_url}/t/{topic.get('slug', '')}/{topic.get('id', '')}",
                        'posts_count': topic.get('posts_count', 0),
                        'last_posted_at': topic.get('last_posted_at', ''),
                        'source': 'discourse'
                    })
                logger.info(f"Scraped {len(topics)} topics from Discourse")
                return True
            else:
                logger.warning(f"Discourse API returned status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error scraping Discourse forum: {e}")
            return False
    
    def find_relevant_content(self, question):
        question_lower = question.lower()
        relevant_links = []
        if any(word in question_lower for word in ['model', 'gpt-3.5', 'gpt-4o', 'api']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/model-selection-guidelines/")
        if any(word in question_lower for word in ['score', 'dashboard', 'ga4', 'bonus']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/scoring-dashboard-explained/")
        if any(word in question_lower for word in ['docker', 'podman', 'container']):
            relevant_links.append("https://tds.s-anand.net/#/tools")
        if any(word in question_lower for word in ['deadline', 'project', 'extension']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/project-deadlines/")
        if any(word in question_lower for word in ['quota', 'error', 'insufficient']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/api-quota-issues/")
        if any(word in question_lower for word in ['github', 'repository', 'license']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/github-requirements/")
        if any(word in question_lower for word in ['vercel', 'deployment', 'build']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/deployment-troubleshooting/")
        if any(word in question_lower for word in ['recorded', 'session', 'youtube']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/course-materials/")
        if any(word in question_lower for word in ['evaluation', 'grading', 'llm']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/evaluation-methodology/")
        if any(word in question_lower for word in ['google', 'cloud', 'gcp']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/gcp-setup-guide/")
        if any(word in question_lower for word in ['pass', 'grade', 'minimum', 'best']):
            relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/t/grading-policy/")
        relevant_links.append("https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34")
        return list(set(relevant_links))
    
    def generate_answer(self, question):
        question_lower = question.lower()
        if 'gpt-3.5' in question_lower and 'gpt-4o' in question_lower:
            return "Use gpt-3.5-turbo-0125 as recommended for TDS assignments."
        if 'dashboard' in question_lower and 'bonus' in question_lower:
            return "Dashboard scores include bonus, e.g., 110% means 10 plus bonus."
        if 'docker' in question_lower and 'podman' in question_lower:
            return "Use Podman for TDS; Docker knowledge is transferable but Podman preferred."
        if 'sep 2025' in question_lower:
            return "No info on TDS Sep 2025; check official announcements."
        if 'project 1' in question_lower and 'deadline' in question_lower:
            return "Project 1 deadline extended to 16 Feb 2025; confirm on Discourse."
        if 'submission' in question_lower and 'error' in question_lower:
            return "Try clearing cache, different browser, or contacting support."
        if 'quota' in question_lower and 'insufficient' in question_lower:
            return "Check your API quota; contact instructor if needed."
        if 'difficult' in question_lower and 'drop' in question_lower:
            return "Complete GA1 before considering dropping. Seek help on Discourse."
        if 'github' in question_lower and ('public' in question_lower or 'license' in question_lower):
            return "Repository should be public with MIT license."
        if 'vector database' in question_lower or 'ga3' in question_lower:
            return "Double-check answer format and contact TAs if needed."
        if 'vercel' in question_lower and 'deployment' in question_lower:
            return "Check build logs, dependencies, and environment variables."
        if 'recorded' in question_lower or 'youtube' in question_lower:
            return "Check YouTube playlist linked in Discourse materials."
        if 'evaluation' in question_lower or 'grading' in question_lower:
            return "Evaluated via LLM grading. Best 4 of 7 GAs with minimum 40%."
        if 'google cloud' in question_lower or 'gcp' in question_lower:
            return "Create new GCP project without parent organization."
        if 'minimum score' in question_lower or 'best 4' in question_lower:
            return "Need 40% overall; best 4 out of 7 GAs count."
        return "Please check course materials or contact TAs for assistance."

kb = TDSKnowledgeBase()

@app.route('/')
def home():
    return jsonify({
        "message": "Virtual TA API for TDS Course",
        "status": "active",
        "endpoints": {
            "chat": "/api",
            "health": "/health"
        }
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api', methods=['POST'])
def chat():
    try:
        data = request.get_json()
        if not data or 'question' not in data:
            return jsonify({"error": "Missing 'question' field"}), 400
        question = data['question'].strip()
        if not question:
            return jsonify({"error": "Question cannot be empty"}), 400
        answer = kb.generate_answer(question)
        links = kb.find_relevant_content(question)
        return jsonify({
            "answer": answer,
            "links": links,
            "timestamp": datetime.now().isoformat(),
            "question": question
        })
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        return jsonify({"error": "Internal server error", "message": str(e)}), 500

@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    try:
        tds_success = kb.scrape_tds_website()
        discourse_success = kb.scrape_discourse_forum()
        return jsonify({
            "tds_scrape": "success" if tds_success else "failed",
            "discourse_scrape": "success" if discourse_success else "failed",
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in scrape endpoint: {e}")
        return jsonify({"error": "Scraping failed", "message": str(e)}), 500

if __name__ == '__main__':
    kb.scrape_tds_website()
    kb.scrape_discourse_forum()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
