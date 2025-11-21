#!/bin/bash

# DFW Openings Setup Script

echo "🚀 Setting up DFW Openings..."

# 1. Check for Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install it first."
    exit 1
fi

# 2. Create virtual environment
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .venv
else
    echo "✅ Virtual environment already exists."
fi

# 3. Activate and install dependencies
echo "⬇️ Installing dependencies..."
source .venv/bin/activate
pip install -r requirements.txt

# 4. Create .env if not exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env from example..."
    cp .env.example .env
else
    echo "✅ .env already exists."
fi

echo "
🎉 Setup complete!

To run the pipeline:
  source .venv/bin/activate
  python run_etl.py

To run the dashboard:
  source .venv/bin/activate
  streamlit run dashboard.py
"
