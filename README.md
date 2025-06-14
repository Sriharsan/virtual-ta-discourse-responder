# TDS Virtual TA

A virtual Teaching Assistant for the Tools in Data Science course at IIT Madras. This application automatically answers student questions based on course content and Discourse forum discussions.

## Features

- **Intelligent Q&A**: Uses OpenAI GPT to provide accurate answers to student questions
- **Knowledge Base**: Built from course content and Discourse forum posts
- **Image Support**: Can process questions with attached images
- **RESTful API**: Easy to integrate with other applications
- **Discourse Scraper**: Bonus script to scrape forum posts by date range

## Quick Start

### Prerequisites

- Python 3.9+
- OpenAI API Key

### Installation

1. Clone this repository:
```bash
git clone https://github.com/Sriharsan/virtual-ta-discourse-responder.git
cd virtual-ta-discourse-responder
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set environment variables:
```bash
export OPENAI_API_KEY="your-openai-api-key-here"
```

4. Run the application:
```bash
python app.py
```

The API will be available at `http://localhost:5000`

## API Usage

### Answer Questions

**Endpoint**: `POST /api/`

**Request Body**:
```json
{
  "question": "Should I use gpt-4o-mini which AI proxy supports, or gpt3.5 turbo?",
  "image": "base64_encoded_image_optional"
}
```

**Response**:
```json
{
  "answer": "You must use `gpt-3.5-turbo-0125`, even if the AI Proxy only supports `gpt-4o-mini`. Use the OpenAI API directly for this question.",
  "links": [
    {
      "url": "https://discourse.onlinedegree.iitm.ac.in/t/ga5-question-8-clarification/155939/4",
      "text": "Use the model that's mentioned in the question."
    }
  ]
}
```

### Example cURL Request

```bash
curl "http://localhost:5000/api/" \
  -H "Content-Type: application/json" \
  -d "{\"question\": \"Should I use gpt-4o-mini which AI proxy supports, or gpt3.5 turbo?\", \"image\": \"$(base64 -w0 project-tds-virtual-ta-q1.webp)\"}"
```

### Health Check

**Endpoint**: `GET /health`

Returns the health status of the API.

## Discourse Scraper 

The included `discourse_scraper.py` script can scrape Discourse posts across a date range:

```bash
python discourse_scraper.py \
  --url "https://discourse.onlinedegree.iitm.ac.in" \
  --start-date "2025-01-01" \
  --end-date "2025-04-14" \
  --categories "Tools in Data Science" \
  --output-json "scraped_posts.json"
```

### Scraper Options

- `--url`: Discourse forum base URL
- `--start-date`: Start date (YYYY-MM-DD)
- `--end-date`: End date (YYYY-MM-DD)
- `--api-key`: Discourse API key (optional)
- `--username`: Discourse username (optional)
- `--categories`: Filter by category names
- `--output-json`: Save to JSON file
- `--db-path`: Database file path

## Deployment

## Local Development
For local testing and development, follow the installation steps above.
## Render Deployment (Recommended)
This application is optimized for deployment on Render:

- Fork/Clone this repository to your GitHub account
- Create a new Web Service on Render
- Connect your GitHub repository
- Configure Environment Variables:

- OPENAI_API_KEY: Your OpenAI API key
- PYTHON_VERSION: 3.9 (optional)


- Deploy: Render will automatically detect and deploy your Flask application

## Render Configuration

- Build Command: pip install -r requirements.txt
- Start Command: ```python app.py``` or ```gunicorn app:app```
- Environment: Python 3

## Alternative Platforms

- Heroku: Use the included Procfile
- Railway: Direct GitHub deployment
- PythonAnywhere: Upload files and configure WSGI

### Using Gunicorn

```bash
gunicorn --bind 0.0.0.0:5000 --workers 2 app:app
```

## Environment Variables

- `OPENAI_API_KEY`: Your OpenAI API key (required)
- `PORT`: Port number (default: 5000)

## Database Schema

The application uses SQLite with two main tables:

### discourse_posts
- `id`: Primary key
- `title`: Post title
- `content`: Cleaned post content
- `url`: Direct link to post
- `category`: Forum category
- `created_at`: Post creation date

### course_content  
- `id`: Primary key
- `title`: Content title
- `content`: Course material content
- `url`: Source URL
- `section`: Course section

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Student       │───▶│   Flask API      │───▶│   OpenAI GPT    │
│   Question      │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   SQLite DB      │
                       │   - Discourse    │
                       │   - Course       │
                       │     Content      │
                       └──────────────────┘
```

## Development

### Project Structure

```
virtual-ta-discourse-responder/
├── .gitignore                              # Git ignore rules
├── LICENSE                                 # MIT License
├── Procfile                               # Heroku deployment config
├── README.md                              # Project documentation
├── app.py                                 # Main Flask application
├── deploy_script.py                       # Deployment automation script
├── discourse_scraper.py                   # Forum scraping utility
├── project-tds-virtual-ta-promptfoo.yaml  # Evaluation configuration
├── requirements.txt                       # Python dependencies
├── runtime.txt                           # Python version specification
└── test_api.py                           # API testing script
```

### Adding New Data Sources

To add new knowledge sources:

1. Create a scraper function in `app.py`
2. Store data in the appropriate database table
3. Update the search function to include new sources

### Testing

Test the API endpoints:

```bash
# Health check
curl -X GET https://virtual-ta-discourse-responder.onrender.com/health

# Test question
curl -X POST https://virtual-ta-discourse-responder.onrender.com/api
-H "Content-Type: application/json"
-d '{"question": "What is the difference between pandas and numpy?"}'
```


### Running Evaluation

1. Update `project-tds-virtual-ta-promptfoo.yaml` with your API URL
2. Run evaluation:
```bash
npx -y promptfoo eval --config project-tds-virtual-ta-promptfoo.yaml
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- IIT Madras Online Degree Program
- Tools in Data Science course instructors
- OpenAI for GPT API
- Flask community

## Support

For questions or issues:
1. Check existing GitHub issues
2. Create a new issue with detailed description
3. Include error logs and steps to reproduce

---

**Note**: This is a student project for educational purposes. Ensure you have proper permissions before scraping any Discourse forum.
