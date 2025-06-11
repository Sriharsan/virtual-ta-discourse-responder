#!/usr/bin/env python3
"""
Test script for the Virtual TA API
"""

import requests
import json
from datetime import datetime

def test_api_endpoint(base_url, question):
    """Test a single API endpoint"""
    url = f"{base_url}/api/chat"
    
    payload = {
        "question": question
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print(f"\n🧪 Testing question: {question[:50]}...")
        
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Success!")
            print(f"Answer: {data.get('answer', 'No answer field')[:100]}...")
            print(f"Links: {len(data.get('links', []))} links provided")
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON response: {e}")
        print(f"Response text: {response.text}")
        return False

def test_health_endpoint(base_url):
    """Test the health endpoint"""
    url = f"{base_url}/health"
    
    try:
        print(f"\n🏥 Testing health endpoint...")
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print("✅ Health check passed!")
            data = response.json()
            print(f"Status: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def main():
    """Main test function"""
    # Test both local and deployed versions
    base_urls = [
        "http://localhost:5000",  # Local development
        "https://virtual-ta-discourse-responder.onrender.com"  # Deployed
    ]
    
    # Test questions from your promptfoo config
    test_questions = [
        "The question asks to use gpt-3.5-turbo-0125 model but the ai-proxy provided by Anand sir only supports gpt-4o-mini. So should we just use gpt-4o-mini or use the OpenAI API for gpt3.5 turbo?",
        "If a student scores 10/10 on GA4 as well as a bonus, how would it appear on the dashboard?",
        "I know Docker but have not used Podman before. Should I use Docker for this course?",
        "Has the Project 1 deadline been extended? I see conflicting dates in different announcements.",
        "How are TDS projects evaluated? Are they manually graded or automatically evaluated using LLMs?"
    ]
    
    for base_url in base_urls:
        print(f"\n{'='*60}")
        print(f"Testing API at: {base_url}")
        print(f"{'='*60}")
        
        # Test health endpoint first
        health_ok = test_health_endpoint(base_url)
        
        if not health_ok:
            print(f"⚠️ Skipping {base_url} - health check failed")
            continue
        
        # Test chat endpoints
        success_count = 0
        for question in test_questions:
            if test_api_endpoint(base_url, question):
                success_count += 1
        
        print(f"\n📊 Results for {base_url}:")
        print(f"✅ Successful tests: {success_count}/{len(test_questions)}")
        print(f"Success rate: {(success_count/len(test_questions)*100):.1f}%")
    
    print(f"\n🎯 Testing completed at {datetime.now()}")

if __name__ == "__main__":
    main()
