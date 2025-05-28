from flask import Flask, request, jsonify
import openai
import base64
import json
import os
from datetime import datetime
import sqlite3
import requests
from typing import List, Dict, Optional
import re
from dataclasses import dataclass

app = Flask(__name__)

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

@dataclass
class DiscoursePost:
    title: str
    content: str
    url: str
    created_at: str
    category: str

class TDSVirtualTA:
    def __init__(self):
        self.db_path = "tds_knowledge.db"
        self.init_database()
        
    def init_database(self):
        """Initialize SQLite database for storing course content and discourse posts"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS course_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                url TEXT,
                section TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discourse_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                content TEXT,
                url TEXT,
                category TEXT,
                created_at TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def scrape_discourse_posts(self, base_url: str, start_date: str, end_date: str) -> List[DiscoursePost]:
        """
        Scrape Discourse posts between given dates
        This is a simplified version - actual implementation would need proper authentication
        and handling of Discourse API
        """
        posts = []
        
        # Sample implementation - in reality, you'd use Discourse API
        # discourse_api_url = f"{base_url}/posts.json"
        
        # For demonstration, we'll create some sample posts
        sample_posts = [
            DiscoursePost(
                title="GA5 Question 8 Clarification",
                content="Use the model that's mentioned in the question. You must use gpt-3.5-turbo-0125 even if AI Proxy supports gpt-4o-mini.",
                url="https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/4",
                created_at="2025-04-10",
                category="assignments"
            ),
            DiscoursePost(
                title="Token Counting for LLM Costs",
                content="My understanding is that you just have to use a tokenizer, similar to what Prof. Anand used, to get the number of tokens and multiply that by the given rate.",
                url="https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/3",  
                created_at="2025-04-09",
                category="assignments"
            )
        ]
        
        return sample_posts
    
    def store_discourse_posts(self, posts: List[DiscoursePost]):
        """Store scraped discourse posts in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for post in posts:
            cursor.execute('''
                INSERT OR REPLACE INTO discourse_posts 
                (title, content, url, category, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (post.title, post.content, post.url, post.category, post.created_at))
        
        conn.commit()
        conn.close()
    
    def search_knowledge_base(self, query: str) -> List[Dict]:
        """Search through stored knowledge base"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Simple text search - in production, you'd use proper full-text search
        search_terms = query.lower().split()
        
        results = []
        
        # Search discourse posts
        cursor.execute('''
            SELECT title, content, url FROM discourse_posts 
            WHERE LOWER(title || ' ' || content) LIKE ?
        ''', (f'%{" ".join(search_terms)}%',))
        
        for row in cursor.fetchall():
            results.append({
                'title': row[0],
                'content': row[1],
                'url': row[2],
                'source': 'discourse'
            })
        
        # Search course content
        cursor.execute('''
            SELECT title, content, url FROM course_content 
            WHERE LOWER(title || ' ' || content) LIKE ?
        ''', (f'%{" ".join(search_terms)}%',))
        
        for row in cursor.fetchall():
            results.append({
                'title': row[0],
                'content': row[1], 
                'url': row[2],
                'source': 'course_content'
            })
        
        conn.close()
        return results
    
    def generate_answer(self, question: str, image_base64: Optional[str] = None) -> Dict:
        """Generate answer using OpenAI API with retrieved context"""
        
        # Search knowledge base
        relevant_docs = self.search_knowledge_base(question)
        
        # Prepare context
        context = ""
        links = []
        
        for doc in relevant_docs[:5]:  # Limit to top 5 results
            context += f"Title: {doc['title']}\nContent: {doc['content']}\nURL: {doc['url']}\n\n"
            links.append({
                "url": doc['url'],
                "text": doc['title']
            })
        
        # Prepare messages for OpenAI
        messages = [
            {
                "role": "system", 
                "content": """You are a helpful Teaching Assistant for the Tools in Data Science course at IIT Madras. 
                Answer student questions based on the provided context from course materials and discourse posts.
                Be specific, accurate, and helpful. If you mention specific tools, models, or versions, be precise.
                Keep answers concise but complete."""
            },
            {
                "role": "user",
                "content": f"""Context from course materials and discussions:
{context}

Student Question: {question}

Please provide a helpful answer based on the context above."""
            }
        ]
        
        # Handle image if provided
        if image_base64:
            # For GPT-4V, we would add image to messages
            # This is simplified - actual implementation would handle vision models
            pass
        
        try:
            if OPENAI_API_KEY:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=messages,
                    max_tokens=500,
                    temperature=0.3
                )
                answer = response.choices[0].message.content
            else:
                # Fallback answer when no API key
                answer = "I can help with TDS course questions. Please ensure the OpenAI API key is configured."
                
        except Exception as e:
            answer = f"I'm having trouble generating a response right now. Please try again later. Error: {str(e)}"
        
        return {
            "answer": answer,
            "links": links[:3]  # Return top 3 relevant links
        }

# Initialize the virtual TA
virtual_ta = TDSVirtualTA()

# Load sample data
sample_posts = virtual_ta.scrape_discourse_posts(
    "https://discourse.onlinedegree.iitm.ac.in", 
    "2025-01-01", 
    "2025-04-14"
)
virtual_ta.store_discourse_posts(sample_posts)

@app.route('/api/', methods=['POST'])
def answer_question():
    """Main API endpoint for answering student questions"""
    try:
        data = request.get_json()
        
        if not data or 'question' not in data:
            return jsonify({"error": "Question is required"}), 400
        
        question = data['question']
        image_base64 = data.get('image')
        
        # Generate answer
        result = virtual_ta.generate_answer(question, image_base64)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/', methods=['GET'])
def home():
    """Home page with API documentation"""
    return jsonify({
        "message": "TDS Virtual TA API",
        "endpoints": {
            "POST /api/": "Answer student questions",
            "GET /health": "Health check"
        },
        "example_request": {
            "question": "Should I use gpt-4o-mini which AI proxy supports, or gpt3.5 turbo?",
            "image": "base64_encoded_image_optional"
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
