#!/usr/bin/env python3
"""
Deployment helper script for TDS Virtual TA
Supports multiple platforms: Railway, Heroku, Google Cloud Run
"""

import os
import subprocess
import sys
import json
from typing import Dict, Optional

class DeploymentHelper:
    def __init__(self):
        self.platforms = {
            'railway': self.deploy_railway,
            'heroku': self.deploy_heroku,
            'gcp': self.deploy_gcp,
            'local': self.run_local
        }
    
    def check_prerequisites(self) -> Dict[str, bool]:
        """Check if required tools are installed"""
        tools = {
            'docker': self.command_exists('docker'),
            'git': self.command_exists('git'),
            'railway': self.command_exists('railway'),
            'heroku': self.command_exists('heroku'),
            'gcloud': self.command_exists('gcloud')
        }
        return tools
    
    def command_exists(self, command: str) -> bool:
        """Check if a command exists in PATH"""
        try:
            subprocess.run([command, '--version'], 
                         capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def get_env_vars(self) -> Dict[str, str]:
        """Get required environment variables"""
        env_vars = {}
        
        # OpenAI API Key
        openai_key = os.getenv('OPENAI_API_KEY')
        if not openai_key:
            openai_key = input("Enter your OpenAI API key: ").strip()
        env_vars['OPENAI_API_KEY'] = openai_key
        
        return env_vars
    
    def deploy_railway(self, env_vars: Dict[str, str]) -> bool:
        """Deploy to Railway"""
        print("🚂 Deploying to Railway...")
        
        try:
            # Initialize Railway project
            subprocess.run(['railway', 'login'], check=True)
            subprocess.run(['railway', 'init'], check=True)
            
            # Set environment variables
            for key, value in env_vars.items():
                subprocess.run(['railway', 'variables', 'set', f'{key}={value}'], 
                             check=True)
            
            # Deploy
            subprocess.run(['railway', 'up'], check=True)
            
            # Get URL
            result = subprocess.run(['railway', 'domain'], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Deployed successfully!")
                print(f"🌐 URL: {result.stdout.strip()}")
                return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Railway deployment failed: {e}")
            return False
    
    def deploy_heroku(self, env_vars: Dict[str, str]) -> bool:
        """Deploy to Heroku"""
        print("🔷 Deploying to Heroku...")
        
        app_name = input("Enter Heroku app name (or press Enter for auto-generated): ").strip()
        
        try:
            # Login to Heroku
            subprocess.run(['heroku', 'login'], check=True)
            
            # Create app
            if app_name:
                subprocess.run(['heroku', 'create', app_name], check=True)
            else:
                subprocess.run(['heroku', 'create'], check=True)
            
            # Set environment variables
            for key, value in env_vars.items():
                subprocess.run(['heroku', 'config:set', f'{key}={value}'], 
                             check=True)
            
            # Deploy
            subprocess.run(['git', 'push', 'heroku', 'main'], check=True)
            
            print("✅ Deployed successfully!")
            print("🌐 Use 'heroku open' to view your app")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Heroku deployment failed: {e}")
            return False
    
    def deploy_gcp(self, env_vars: Dict[str, str]) -> bool:
        """Deploy to Google Cloud Run"""
        print("☁️ Deploying to Google Cloud Run...")
        
        project_id = input("Enter your GCP Project ID: ").strip()
        service_name = input("Enter service name (default: tds-virtual-ta): ").strip() or "tds-virtual-ta"
        region = input("Enter region (default: us-central1): ").strip() or "us-central1"
        
        try:
            # Set project
            subprocess.run(['gcloud', 'config', 'set', 'project', project_id], 
                         check=True)
            
            # Build and deploy
            env_flags = []
            for key, value in env_vars.items():
                env_flags.extend(['--set-env-vars', f'{key}={value}'])
            
            cmd = [
                'gcloud', 'run', 'deploy', service_name,
                '--source', '.',
                '--platform', 'managed',
                '--region', region,
                '--allow-unauthenticated'
            ] + env_flags
            
            subprocess.run(cmd, check=True)
            
            print("✅ Deployed successfully!")
            print(f"🌐 Your service is available at: https://{service_name}-{region}.run.app")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ GCP deployment failed: {e}")
            return False
    
    def run_local(self, env_vars: Dict[str, str]) -> bool:
        """Run locally for testing"""
        print("🏠 Running locally...")
        
        # Set environment variables
        for key, value in env_vars.items():
            os.environ[key] = value
        
        try:
            # Install dependencies
            subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                         check=True)
            
            # Initialize database
            print("Initializing database...")
            subprocess.run([sys.executable, '-c', 
                          'from app import virtual_ta; print("Database initialized")'], 
                         check=True)
            
            print("✅ Starting server...")
            print("🌐 API will be available at: http://localhost:5000")
            print("📊 Health check: http://localhost:5000/health")
            print("📖 API docs: http://localhost:5000/")
            print("\nPress Ctrl+C to stop")
            
            # Run the app
            subprocess.run([sys.executable, 'app.py'])
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Local run failed: {e}")
            return False
        except KeyboardInterrupt:
            print("\n👋 Server stopped")
            return True
    
    def setup_sample_data(self):
        """Setup sample data for testing"""
        print("📝 Setting up sample data...")
        
        try:
            # Run sample data script
            subprocess.run([sys.executable, '-c', '''
from app import virtual_ta

# Add sample course content
import sqlite3
conn = sqlite3.connect("tds_knowledge.db")
cursor = conn.cursor()

sample_content = [
    ("Python Basics", "Python is a high-level programming language. Use pandas for data manipulation.", 
     "https://course.example.com/python", "Week 1"),
    ("Machine Learning", "Use scikit-learn for ML. Random Forest is good for classification.", 
     "https://course.example.com/ml", "Week 5"),
    ("Data Visualization", "Use matplotlib and seaborn for plotting. Always label your axes.", 
     "https://course.example.com/viz", "Week 3")
]

for title, content, url, section in sample_content:
    cursor.execute("""
        INSERT OR REPLACE INTO course_content (title, content, url, section)
        VALUES (?, ?, ?, ?)
    """, (title, content, url, section))

conn.commit()
conn.close()
print("Sample data added!")
            '''], check=True)
            
            print("✅ Sample data setup complete!")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Sample data setup failed: {e}")
    
    def main(self):
        """Main deployment workflow"""
        print("🚀 TDS Virtual TA Deployment Helper")
        print("=" * 40)
        
        # Check prerequisites
        tools = self.check_prerequisites()
        print("\n🔍 Checking prerequisites:")
        for tool, available in tools.items():
            status = "✅" if available else "❌"
            print(f"  {status} {tool}")
        
        # Get environment variables
        print("\n🔑 Setting up environment...")
        env_vars = self.get_env_vars()
        
        # Choose platform
        print("\n🎯 Choose deployment platform:")
        platforms = list(self.platforms.keys())
        for i, platform in enumerate(platforms, 1):
            print(f"  {i}. {platform.title()}")
        
        while True:
            try:
                choice = int(input("\nEnter your choice (1-4): ")) - 1
                if 0 <= choice < len(platforms):
                    platform = platforms[choice]
                    break
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
        
        # Setup sample data for local testing
        if platform == 'local':
            setup_data = input("\nSetup sample data for testing? (y/n): ").lower() == 'y'
            if setup_data:
                self.setup_sample_data()
        
        # Deploy
        print(f"\n🚀 Starting {platform} deployment...")
        success = self.platforms[platform](env_vars)
        
        if success:
            print("\n🎉 Deployment completed successfully!")
            
            if platform != 'local':
                print("\n📋 Next steps:")
                print("1. Test your API endpoint")
                print("2. Update promptfoo config with your URL")
                print("3. Run evaluation tests")
                print("4. Submit your GitHub repo and API URL")
        else:
            print("\n😞 Deployment failed. Please check the errors above.")

if __name__ == "__main__":
    helper = DeploymentHelper()
    helper.main()
