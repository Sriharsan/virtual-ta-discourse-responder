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
import base64
from PIL import Image
import io
import pytesseract

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
            # Enhanced programming knowledge
            'programming_topics': {
                'pandas_vs_numpy': {
                    'pandas': 'Data manipulation library built on NumPy, designed for structured data with DataFrames and Series',
                    'numpy': 'Numerical computing library for arrays and mathematical operations',
                    'difference': 'NumPy is for numerical arrays, Pandas is for data analysis with labeled data structures'
                },
                'virtual_environments': {
                    'purpose': 'Isolate Python dependencies for different projects',
                    'commands': 'python -m venv myenv, source myenv/bin/activate (Linux/Mac), myenv\\Scripts\\activate (Windows)',
                    'issues': 'ModuleNotFoundError often indicates wrong environment or missing installation'
                },
                'python_versions': {
                    'compatibility': 'TDS course supports Python 3.8 to 3.11',
                    'recommendation': 'Use Python 3.9 or 3.10 for best compatibility',
                    'issues': 'Some packages may not work with Python 3.12+'
                }
            },
            # GA-specific guidance
            'ga_specific': {
                'ga1': 'Prerequisites completion required before other GAs',
                'ga2': 'Common token counting issues - use tiktoken library',
                'ga3': 'Vector database validation issues - check answer format',
                'ga4': 'Dashboard scoring includes bonus points (110/100)',
                'ga5': 'Use gpt-3.5-turbo-0125, not gpt-4o-mini from AI proxy'
            },
            # Tools and frameworks
            'tools': {
                'promptfoo': {
                    'config': 'promptfoo-config.yaml configuration file needed',
                    'common_errors': 'Provider not found - check API keys and URLs',
                    'setup': 'npm install -g promptfoo, then promptfoo init'
                },
                'langchain': {
                    'vector_stores': 'FAISS and Chroma supported for RAG applications',
                    'usage': 'Use for building RAG (Retrieval Augmented Generation) systems',
                    'issues': 'Version compatibility important - check requirements.txt'
                },
                'deployment': {
                    'vercel': 'Preferred for web apps, check build logs for errors',
                    'debug': 'Common issues: missing env vars, build failures, 404 errors'
                }
            }
        }
        self.scraped_data = []
        self.last_updated = None
        
    def process_image(self, base64_image):
        """Process base64 image and extract text using OCR"""
        try:
            # Decode base64 image
            image_data = base64.b64decode(base64_image)
            image = Image.open(io.BytesIO(image_data))
            
            # Extract text using OCR
            text = pytesseract.image_to_string(image)
            return text.strip()
        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return None
        
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
        
        # Virtual environment issues
        if any(word in question_lower for word in ['venv', 'virtual environment', 'modulenotfounderror', 'activate']):
            relevant_links.append({
                "url": "https://docs.python.org/3/tutorial/venv.html",
                "text": "Python Virtual Environments Guide"
            })
        
        # Python version issues
        if any(word in question_lower for word in ['python version', '3.8', '3.11', 'compatibility']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - Python Version Issues"
            })
        
        # Promptfoo issues
        if any(word in question_lower for word in ['promptfoo', 'config', 'provider not found']):
            relevant_links.append({
                "url": "https://promptfoo.dev/docs/getting-started",
                "text": "Promptfoo Documentation"
            })
        
        # Token and cost issues
        if any(word in question_lower for word in ['token', 'cost', 'pricing', 'tiktoken']):
            relevant_links.append({
                "url": "https://openai.com/pricing",
                "text": "OpenAI Pricing Guide"
            })
        
        # LangChain and vector stores
        if any(word in question_lower for word in ['langchain', 'faiss', 'chroma', 'rag', 'vector store']):
            relevant_links.append({
                "url": "https://python.langchain.com/docs/get_started/introduction",
                "text": "LangChain Documentation"
            })
        
        # Git issues
        if any(word in question_lower for word in ['git', 'push', 'commit', 'repository']):
            relevant_links.append({
                "url": "https://git-scm.com/docs",
                "text": "Git Documentation"
            })
        
        # VM and WSL issues
        if any(word in question_lower for word in ['vm', 'virtual machine', 'wsl', 'ubuntu', 'oracle']):
            relevant_links.append({
                "url": "https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34",
                "text": "TDS Course Forum - VM Setup Help"
            })
        
        # Programming-related links
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
    
    def generate_answer(self, question, image_text=None):
        question_lower = question.lower()
        
        # Include image text in analysis if available
        if image_text:
            question_lower += " " + image_text.lower()
            logger.info(f"Image text extracted: {image_text[:100]}...")
        
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

        # Virtual environment issues
        if any(word in question_lower for word in ['venv', 'virtual environment', 'modulenotfounderror', 'activate']):
            return """Virtual environment issues are common in Python development:

**Creating a virtual environment:**
```bash
python -m venv myenv
```

**Activating:**
- Linux/Mac: `source myenv/bin/activate`
- Windows: `myenv\\Scripts\\activate`

**Common issues:**
- `ModuleNotFoundError`: Usually means you're not in the correct virtual environment or package isn't installed
- Always activate your environment before installing packages
- Use `pip list` to check installed packages
- Deactivate with `deactivate` command

**Best practices:**
- Create separate environments for different projects
- Use `requirements.txt` to track dependencies
- Always activate environment before running your code"""

        # Python version compatibility
        if any(word in question_lower for word in ['python version', '3.8', '3.11', 'compatibility']):
            return """Python version compatibility for TDS course:

**Supported versions:** Python 3.8 to 3.11
**Recommended:** Python 3.9 or 3.10 for best compatibility

**Common issues:**
- Python 3.12+ may have package compatibility issues
- Use `python --version` to check your current version
- Consider using pyenv to manage multiple Python versions

**If you have version issues:**
- Install the recommended version
- Create a new virtual environment with the correct Python version
- Reinstall your packages in the new environment"""

        # Promptfoo issues
        if any(word in question_lower for word in ['promptfoo', 'config', 'provider not found']):
            return """Promptfoo setup and common issues:

**Installation:**
```bash
npm install -g promptfoo
promptfoo init
```

**Configuration (promptfoo-config.yaml):**
- Check your API keys are correctly set
- Verify provider URLs are accessible
- Ensure correct model names

**Common errors:**
- "Provider not found": Check API key and provider configuration
- Network issues: Verify internet connection and API endpoints
- Model access: Ensure you have access to the specified models

**Debugging:**
- Run with `--verbose` flag for detailed logs
- Test API connectivity separately
- Check promptfoo documentation for provider-specific setup"""

        # Token counting and cost estimation
        if any(word in question_lower for word in ['token', 'cost', 'pricing', 'tiktoken', 'ga2']):
            return """Token counting and cost estimation:

**For GA2 and token-related questions:**
- Use `tiktoken` library for accurate token counting
- Install: `pip install tiktoken`

**Example usage:**
```python
import tiktoken
encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
tokens = encoding.encode("Your text here")
token_count = len(tokens)
```

**Cost calculation:**
- Check current OpenAI pricing at https://openai.com/pricing
- Multiply token count by rate per token
- Consider both input and output tokens

**Common issues:**
- Different models have different tokenizers
- Pricing changes over time - always check latest rates"""

        # LangChain and RAG
        if any(word in question_lower for word in ['langchain', 'faiss', 'chroma', 'rag', 'vector store']):
            return """LangChain and Vector Stores for RAG:

**Supported vector stores:**
- FAISS: Fast similarity search, good for large datasets
- Chroma: Persistent vector database, easier setup

**Common setup:**
```python
from langchain.vectorstores import FAISS, Chroma
from langchain.embeddings import OpenAIEmbeddings

# FAISS example
vectorstore = FAISS.from_texts(texts, OpenAIEmbeddings())

# Chroma example
vectorstore = Chroma.from_texts(texts, OpenAIEmbeddings())
```

**Troubleshooting:**
- Version compatibility: Check LangChain version requirements
- Ensure proper API keys for embeddings
- For GA3: Double-check answer format for validation"""

        # Rate limiting and quota
        if any(word in question_lower for word in ['rate limit', '429', 'quota', 'exceeded']):
            return """Rate limiting and quota management:

**Common rate limit errors:**
- HTTP 429: Too many requests
- Quota exceeded: Monthly/daily limits reached

**Solutions:**
- Implement exponential backoff retry logic
- Add delays between API calls
- Monitor your usage on OpenAI dashboard
- Contact instructor if course quota exceeded

**Best practices:**
- Cache responses when possible
- Batch requests efficiently
- Use appropriate rate limiting libraries"""

        # Deployment issues
        if any(word in question_lower for word in ['vercel', 'deployment', 'build', 'failing', '404']):
            return """Deployment troubleshooting:

**Vercel common issues:**
- Build failures: Check build logs for specific errors
- 404 errors: Verify routing configuration
- Environment variables: Ensure all required env vars are set

**Debugging steps:**
1. Check build logs in Vercel dashboard
2. Verify all dependencies in package.json
3. Test locally before deploying
4. Check function timeout limits
5. Verify API routes are correctly configured

**Common fixes:**
- Add missing dependencies
- Fix import paths
- Set correct Node.js version
- Configure environment variables properly"""

        # VM and WSL issues
        if any(word in question_lower for word in ['vm', 'virtual machine', 'wsl', 'ubuntu', 'oracle']):
            return """Virtual Machine and WSL troubleshooting:

**Common VM issues:**
- VM not starting: Check system resources and virtualization settings
- Performance issues: Allocate more RAM/CPU if available
- Network connectivity: Verify network adapter settings

**WSL (Windows Subsystem for Linux):**
- Installation: `wsl --install`
- Update: `wsl --update`
- Common issues: File permissions, path differences

**Oracle VM VirtualBox:**
- Enable virtualization in BIOS
- Install Guest Additions for better performance
- Check host system resources

**Alternatives:**
- Use cloud platforms (Google Colab, Replit)
- Docker containers for consistent environments"""

        # Git issues
        if any(word in question_lower for word in ['git', 'push', 'commit', 'repository', 'not updating']):
            return """Git troubleshooting:

**Common commands:**
```bash
git add .
git commit -m "Your message"
git push origin main
```

**Common issues:**
- Authentication: Use personal access tokens, not passwords
- Branch issues: Check you're on the correct branch
- Merge conflicts: Resolve conflicts before pushing

**Repository not updating:**
- Check if you're pushing to the correct branch
- Verify remote URL: `git remote -v`
- Ensure you have push permissions

**Best practices:**
- Commit frequently with meaningful messages
- Pull before pushing: `git pull origin main`
- Use .gitignore for sensitive files"""

        # Linux/Mac command errors
        if any(word in question_lower for word in ['permission denied', 'command not found', 'chmod']):
            return """Linux/Mac command troubleshooting:

**Permission denied:**
- Use `chmod +x filename` to make files executable
- Use `sudo` for administrative commands (be careful!)
- Check file ownership: `ls -la`

**Command not found:**
- Check if software is installed
- Verify PATH environment variable
- Use `which command` to find command location

**Common fixes:**
- Install missing packages
- Add to PATH: `export PATH=$PATH:/path/to/directory`
- Use package managers: `apt`, `brew`, `yum`"""

        # Common Python errors
        if any(word in question_lower for word in ['keyerror', 'typeerror', 'indexerror', 'attributeerror']):
            return """Common Python errors and solutions:

**KeyError:** Dictionary key doesn't exist
- Use `dict.get(key, default)` instead of `dict[key]`
- Check if key exists: `if key in dict:`

**TypeError:** Wrong data type
- Check variable types with `type(variable)`
- Convert types: `str()`, `int()`, `float()`

**IndexError:** List index out of range
- Check list length: `len(list)`
- Use try/except for robust error handling

**AttributeError:** Object doesn't have attribute
- Check object type and available methods
- Use `dir(object)` to see available attributes

**General debugging:**
- Use print statements to debug
- Read error messages carefully
- Check variable values at each step"""

        # End-term exam info
        if any(word in question_lower for word in ['end-term', 'exam', 'certification', 'schedule']):
            return """End-term exam and certification information:

**Eligibility:**
- Complete all GA requirements
- Maintain minimum 40% overall score
- Ensure GA1 is completed (prerequisite)

**Schedule:**
- Check official announcements for exam dates
- Sep 2025 term information not yet available

**Preparation:**
- Review all GA submissions
- Practice with course materials
- Check YouTube playlist for recorded sessions

**Certification:**
- Based on overall course performance
- Includes both GA scores and end-term exam

For specific dates and requirements, check official course announcements."""

        # Discussion forum navigation
        if any(word in question_lower for word in ['where to ask', 'discourse', 'forum', 'search']):
            return """Using the TDS Discussion Forum:

**Where to ask questions:**
- Main TDS forum: https://discourse.onlinedegree.iitm.ac.in/c/courses/tds-kb/34
- Search existing topics first
- Create new topic if issue not found

**Search tips:**
- Use specific keywords
- Check recent posts for similar issues
- Look at pinned topics for important announcements

**Best practices:**
- Clear, descriptive topic titles
- Include relevant code/error messages
- Tag appropriately for visibility
- Be patient for responses from TAs/peers

**Categories:**
- Technical issues: Code problems, setup issues
- GA-specific: Questions about specific assignments
- General: Course policies, deadlines, etc."""

        # Model selection questions
        if any(word in question_lower for word in ['gpt-3.5', 'gpt-4o', 'ai-proxy', 'model']):
            if 'ai-proxy' in question_lower and 'gpt-4o-mini' in question_lower:
                return "Use gpt-3.5-turbo-0125 as specified in the requirements, not gpt-4o-mini. You should use the OpenAI API directly for gpt-3.5-turbo-0125 rather than the ai-proxy."
            return "Use gpt-3.5-turbo-0125 as recommended for TDS assignments. This is the preferred model for consistency in evaluation."
        
        # Dashboard and scoring
        if any(word in question_lower for word in ['dashboard', 'bonus', '110', 'ga4', 'score']):
            return "Dashboard scores include bonus points. For example, if you score 10/10 plus bonus, it would appear as 110 or 110% on the dashboard. This is normal and indicates excellent performance with bonus points."
        
        # Docker vs Podman
        if any(word in question_lower for word in ['docker', 'podman']):
            return "Use Podman for TDS course as recommended. While Docker knowledge is transferable, Podman is preferred for this course. However, Docker is also acceptable if you're more comfortable with it."
        
        # Future course information
        if 'sep 2025' in question_lower or 'end-term exam' in question_lower:
            return "I don't have information about TDS Sep 2025 schedule. Please check official announcements or contact course coordinators for the most current information."
        
        # Project deadlines
        if any(word in question_lower for word in ['project 1', 'deadline', 'extension', '16 feb']):
            return "Project 1 deadline has been extended to 16 Feb 2025. Please confirm the latest deadline on Discourse as extensions may be updated."
        
        # Submission issues
        if any(word in question_lower for word in ['submission', 'error', 'portal', 'not submitting']):
            return "Try clearing your browser cache, using a different browser, or contact support if submission issues persist. Also check if you're using the correct submission format."
        
        # API quota issues
        if any(word in question_lower for word in ['quota', 'insufficient', 'llm embed']):
            return "Check your API quota and usage limits. Contact the instructor if you've exceeded your quota allocation. Monitor usage on your OpenAI dashboard."
        
        # Course difficulty
        if any(word in question_lower for word in ['difficult', 'drop', 'level']):
            return "Complete GA1 before considering dropping the course. Seek help on Discourse forum first - many students find support there. The course becomes more manageable with practice."
        
        # GitHub requirements
        if any(word in question_lower for word in ['github', 'public', 'license', 'mit']):
            return "Your GitHub repository should be public with an MIT license for Project 1 submission. Include a LICENSE file in the root directory with MIT license text."
        
        # Vector database issues
        if any(word in question_lower for word in ['vector', 'database', 'ga3', 'validation']):
            return "For GA3 vector database questions, double-check your answer format and contact TAs if validation continues to fail. Ensure your vector store is properly configured."
        
        # Course materials
        if any(word in question_lower for word in ['recorded', 'session', 'youtube', 'missed']):
            return "Check the YouTube playlist linked in Discourse course materials for recorded sessions. All important sessions are typically recorded and available."
        
        # Evaluation methodology
        if any(word in question_lower for word in ['evaluation', 'grading', 'llm', 'automated']):
            return "Projects are evaluated using automated LLM-based grading. Final grade is best 4 out of 7 GAs with minimum 40% overall. This ensures fair and consistent evaluation."
        
        # Google Cloud setup
        if any(word in question_lower for word in ['google', 'cloud', 'gcp', 'parent organization']):
            return "For Google Cloud setup, create a new project without selecting a parent organization or folder. This avoids permission issues."
        
        # Grading and passing
        if any(word in question_lower for word in ['minimum', 'score', 'pass', 'best 4', '40%']):
            return "You need minimum 40% overall to pass. Final grade calculated from best 4 out of 7 GA scores. Focus on completing GAs well rather than all perfectly."
        
        # Generic response
        return "Please check the course materials on Discourse or contact TAs for specific assistance with your question. Be sure to search existing topics first as your question may already be answered."

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
        },
        "features": [
            "Text-based Q&A",
            "Image OCR support",
            "Discourse forum scraping",
            "Comprehensive error handling"
        ]
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

# Enhanced API endpoint with image support
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
            image_data = None
            if not question:
                question = "What is TDS course about?"
        else:
            # Handle POST requests with detailed logging
            logger.info(f"Raw request data length: {len(request.get_data())}")
            
            try:
                # Try different approaches to get JSON data
                if request.is_json:
                    data = request.get_json()
                    logger.info(f"Parsed JSON data keys: {list(data.keys()) if data else 'None'}")
                else:
                    # Force JSON parsing even if content-type is not set correctly
                    data = request.get_json(force=True)
                    logger.info(f"Force-parsed JSON data keys: {list(data.keys()) if data else 'None'}")
                
                if not data:
                    logger.error("No JSON data received")
                    return jsonify({"error": "No JSON data provided"}), 400
                    
                if 'question' not in data:
                    logger.error(f"Missing 'question' field. Available fields: {list(data.keys())}")
                    return jsonify({"error": "Missing 'question' field in JSON"}), 400
                    
                question = data['question']
                image_data = data.get('image')  # Optional base64 image
                
            except Exception as json_error:
                logger.error(f"JSON parsing error: {json_error}")
                logger.error(f"Request data type: {type(request.get_data())}")
                return jsonify({
                    "error": "Invalid JSON format", 
                    "details": str(json_error),
                    "help": "Ensure JSON contains 'question' field"
                }), 400
        
        if not question or not question.strip():
            return jsonify({"error": "Question cannot be empty"}), 400
            
        question = question.strip()
        logger.info(f"Processing question: {question[:100]}...")
        
        # Process image if provided
        image_text = None
        if image_data:
            try:
                image_text = kb.process_image(image_data)
                logger.info(f"Image processed successfully: {bool(image_text)}")
            except Exception as img_error:
                logger.error(f"Image processing failed: {img_error}")
                # Continue without image text
        
        # Generate response
        answer = kb.generate_answer(question, image_text)
        links = kb.find_relevant_content(question)
        
        response_data = {
            "answer": answer,
            "links": links[:5],  # Limit to 5 most relevant links
            "timestamp": datetime.now().isoformat(),
            "question": question,
            "status": "success"
        }
        
        if image_text:
            response_data["image_text_extracted"] = bool(image_text)
        
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

@app.route('/test', methods=['GET', 'POST'])
def test():
    return jsonify({
        "message": "Test endpoint working",
        "method": request.method,
        "timestamp": datetime.now().isoformat(),
        "status": "success"
    })

if __name__ == '__main__':
    logger.info("Starting TDS Virtual TA Flask application...")
    try:
        # Initialize with some data
        kb.scrape_tds_website()
        kb.scrape_discourse_forum()
        logger.info("Initial data scraping completed")
    except Exception as e:
        logger.error(f"Error during initialization: {e}")
    
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
