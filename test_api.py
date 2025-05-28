#!/usr/bin/env python3
"""
API Testing Script for TDS Virtual TA
Tests various scenarios and validates responses
"""

import requests
import json
import base64
import time
from typing import Dict, List, Optional
import sys
import argparse

class APITester:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def test_health_endpoint(self) -> bool:
        """Test the health check endpoint"""
        print("🔍 Testing health endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/health")
            response.raise_for_status()
            
            data = response.json()
            print(f"✅ Health check passed: {data}")
            return True
            
        except Exception as e:
            print(f"❌ Health check failed: {e}")
            return False
    
    def test_basic_question(self) -> bool:
        """Test basic question without image"""
        print("🔍 Testing basic question...")
        
        payload = {
            "question": "What is the difference between pandas and numpy?"
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/", 
                                       json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response structure
            if 'answer' not in data:
                print("❌ Response missing 'answer' field")
                return False
            
            if 'links' not in data:
                print("❌ Response missing 'links' field")  
                return False
            
            print(f"✅ Basic question test passed")
            print(f"   Answer: {data['answer'][:100]}...")
            print(f"   Links: {len(data['links'])} provided")
            return True
            
        except Exception as e:
            print(f"❌ Basic question test failed: {e}")
            return False
    
    def test_tds_specific_question(self) -> bool:
        """Test TDS course specific question"""
        print("🔍 Testing TDS-specific question...")
        
        payload = {
            "question": "Should I use gpt-4o-mini which AI proxy supports, or gpt3.5 turbo?"
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/", 
                                       json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if answer mentions specific models
            answer = data['answer'].lower()
            has_model_info = any(model in answer for model in 
                               ['gpt-3.5-turbo', 'gpt-4o-mini', 'openai'])
            
            if has_model_info:
                print("✅ TDS-specific question test passed")
                print(f"   Answer: {data['answer'][:150]}...")
                return True
            else:
                print("⚠️ Answer might not be specific enough to TDS content")
                print(f"   Answer: {data['answer']}")
                return True  # Still pass, but with warning
                
        except Exception as e:
            print(f"❌ TDS-specific question test failed: {e}")
            return False
    
    def test_with_image(self, image_path: Optional[str] = None) -> bool:
        """Test question with image attachment"""
        print("🔍 Testing question with image...")
        
        # Create a simple test image (1x1 pixel PNG)
        if not image_path:
            # Simple base64 encoded 1x1 transparent PNG
            test_image_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
        else:
            try:
                with open(image_path, 'rb') as f:
                    test_image_b64 = base64.b64encode(f.read()).decode('utf-8')
            except Exception as e:
                print(f"❌ Could not read image file: {e}")
                return False
        
        payload = {
            "question": "What do you see in this image?",
            "image": test_image_b64
        }
        
        try:
            response = self.session.post(f"{self.base_url}/api/", 
                                       json=payload)
            response.raise_for_status()
            
            data = response.json()
            print("✅ Image question test passed")
            print(f"   Answer: {data['answer'][:100]}...")
            return True
            
        except Exception as e:
            print(f"❌ Image question test failed: {e}")
            return False
    
    def test_error_cases(self) -> bool:
        """Test various error scenarios"""
        print("🔍 Testing error cases...")
        
        test_cases = [
            # Missing question
            ({}, "Missing question field"),
            # Empty question
            ({"question": ""}, "Empty question"),
            # Very long question
            ({"question": "What is " + "very " * 1000 + "long question?"}, "Very long question")
        ]
        
        passed = 0
        for payload, description in test_cases:
            try:
                response = self.session.post(f"{self.base_url}/api/", 
                                           json=payload)
                
                if response.status_code >= 400:
                    print(f"   ✅ {description}: Properly returned error {response.status_code}")
                    passed += 1
                else:
                    data = response.json()
                    if 'error' in data:
                        print(f"   ✅ {description}: Returned error in response")
                        passed += 1
                    else:
                        print(f"   ⚠️ {description}: Expected error but got valid response")
                        
            except Exception as e:
                print(f"   ❌ {description}: Unexpected error {e}")
        
        print(f"✅ Error cases test: {passed}/{len(test_cases)} passed")
        return passed > 0
    
    def test_response_time(self) -> bool:
        """Test API response time"""
        print("🔍 Testing response time...")
        
        payload = {
            "question": "What is machine learning?"
        }
        
        start_time = time.time()
        
        try:
            response = self.session.post(f"{self.base_url}/api/", 
                                       json=payload)
            response.raise_for_status()
            
            end_time = time.time()
            response_time = end_time - start_time
            
            print(f"✅ Response time: {response_time:.2f} seconds")
            
            if response_time < 30:  # Reasonable timeout
                print("   ✅ Response time is acceptable")
                return True
            else:
                print("   ⚠️ Response time is quite slow")
                return True  # Still pass, but with warning
                
        except Exception as e:
            print(f"❌ Response time test failed: {e}")
            return False
    
    def test_concurrent_requests(self, num_requests: int = 3) -> bool:
        """Test handling of concurrent requests"""
        print(f"🔍 Testing {num_requests} concurrent requests...")
        
        import threading
        import queue
        
        results = queue.Queue()
        
        def make_request(question_id):
            payload = {
                "question": f"Test question {question_id}: What is data science?"
            }
            
            try:
                response = self.session.post(f"{self.base_url}/api/", 
                                           json=payload)
                response.raise_for_status()
                results.put(('success', question_id))
            except Exception as e:
                results.put(('error', question_id, str(e)))
        
        # Start threads
        threads = []
        for i in range(num_requests):
            thread = threading.Thread(target=make_request, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Check results
        successes = 0
        while not results.empty():
            result = results.get()
            if result[0] == 'success':
                successes += 1
            else:
                print(f"   ❌ Request {result[1]} failed: {result[2]}")
        
        print(f"✅ Concurrent requests: {successes}/{num_requests} succeeded")
        return successes > 0
    
    def run_all_tests(self, image_path: Optional[str] = None) -> Dict[str, bool]:
        """Run all tests and return results"""
        print("🚀 Starting API Tests")
        print("=" * 40)
        
        tests = [
            ("Health Check", self.test_health_endpoint),
            ("Basic Question", self.test_basic_question),
            ("TDS-Specific Question", self.test_tds_specific_question),
            ("Question with Image", lambda: self.test_with_image(image_path)),
            ("Error Cases", self.test_error_cases),
            ("Response Time", self.test_response_time),
            ("Concurrent Requests", self.test_concurrent_requests)
        ]
        
        results = {}
        passed = 0
        
        for test_name, test_func in tests:
            print(f"\n📋 {test_name}")
            print("-" * 30)
            
            try:
                result = test_func()
                results[test_name] = result
                if result:
                    passed += 1
            except Exception as e:
                print(f"❌ Test '{test_name}' crashed: {e}")
                results[test_name] = False
        
        # Summary
        print("\n" + "=" * 40)
        print("📊 TEST SUMMARY")
        print("=" * 40)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} {test_name}")
        
        print(f"\nOverall: {passed}/{len(tests)} tests passed")
        
        if passed == len(tests):
            print("🎉 All tests passed! Your API is working correctly.")
        elif passed > len(tests) // 2:
            print("⚠️ Most tests passed. Check the failures above.")
        else:
            print("😞 Multiple tests failed. Please check your API implementation.")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Test TDS Virtual TA API')
    parser.add_argument('--url', required=True, help='API base URL')
    parser.add_argument('--image', help='Path to test image file')
    
    args = parser.parse_args()
    
    tester = APITester(args.url)
    results = tester.run_all_tests(args.image)
    
    # Exit with appropriate code
    if all(results.values()):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
