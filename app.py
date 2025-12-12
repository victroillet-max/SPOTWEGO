"""
Restaurant Curation Platform - V3
Features:
- Fixed data collection (no infinite loops)
- Auto-create regions on collection
- Regional rankings and push
- Scalable database structure
- Daily auto-recalculation support
- Flexible push options (per region, top N)
"""

import os
import json
import logging
import math
import random
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import hashlib
import secrets

# Database configuration
DATABASE_URL = os.environ.get('AZURE_POSTGRESQL_CONNECTIONSTRING') or os.environ.get('DATABASE_URL')

# Force SQLite if USE_SQLITE=1 is set (for local development)
FORCE_SQLITE = os.environ.get('USE_SQLITE', '0') == '1'

USE_POSTGRES = False
if DATABASE_URL and not FORCE_SQLITE:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        USE_POSTGRES = True
    except ImportError:
        pass

if not USE_POSTGRES:
    import sqlite3


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_hex(32)
    # Use local ./data folder for development, /home/data for production
    if os.path.exists('./data'):
        SQLITE_PATH = os.environ.get('SQLITE_PATH', './data/spotwego.db')
        CONFIG_PATH = os.environ.get('CONFIG_PATH', './data/config.json')
    else:
        SQLITE_PATH = os.environ.get('SQLITE_PATH', '/home/data/restaurant_platform.db')
        CONFIG_PATH = os.environ.get('CONFIG_PATH', '/home/data/config.json')


def create_app():
    app = Flask(__name__, static_folder='static', template_folder='templates')
    app.config.from_object(Config)
    CORS(app)
    
    if not app.debug:
        logging.basicConfig(level=logging.INFO)
        app.logger.info(f'Starting app - PostgreSQL: {USE_POSTGRES}')
    
    # Global error handlers to always return JSON
    @app.errorhandler(Exception)
    def handle_exception(e):
        app.logger.error(f'Unhandled exception: {str(e)}')
        return jsonify({'error': str(e), 'type': type(e).__name__}), 500
    
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({'error': 'Not found'}), 404
    
    @app.errorhandler(500)
    def server_error(e):
        return jsonify({'error': 'Internal server error'}), 500
    
    init_db()
    register_routes(app)
    return app


def get_db():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        os.makedirs(os.path.dirname(Config.SQLITE_PATH) or '.', exist_ok=True)
        conn = sqlite3.connect(Config.SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        return conn


def get_cursor(conn):
    """Get a cursor that returns dict-like rows"""
    if USE_POSTGRES:
        return conn.cursor(cursor_factory=RealDictCursor)
    else:
        return conn.cursor()


def init_db():
    conn = get_db()
    cur = conn.cursor()
    
    if USE_POSTGRES:
        tables = [
            # API Keys
            """CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                provider VARCHAR(50) UNIQUE NOT NULL,
                api_key_encrypted TEXT,
                is_active INTEGER DEFAULT 1,
                provider_type VARCHAR(50) DEFAULT 'builtin',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
            
            # API Usage
            """CREATE TABLE IF NOT EXISTS api_usage (
                id SERIAL PRIMARY KEY,
                provider VARCHAR(50),
                endpoint VARCHAR(100),
                request_count INTEGER DEFAULT 1,
                estimated_cost REAL DEFAULT 0,
                timestamp TIMESTAMP DEFAULT NOW())""",
            
            # Regions - Enhanced for scaling
            """CREATE TABLE IF NOT EXISTS regions (
                id SERIAL PRIMARY KEY,
                code VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(100),
                country VARCHAR(100) DEFAULT 'Switzerland',
                canton VARCHAR(50),
                latitude REAL,
                longitude REAL,
                zoom_level INTEGER DEFAULT 10,
                last_scan TIMESTAMP,
                restaurants_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                auto_push INTEGER DEFAULT 0,
                push_top_n INTEGER DEFAULT 20,
                created_at TIMESTAMP DEFAULT NOW())""",
            
            # Restaurants - Enhanced with email
            """CREATE TABLE IF NOT EXISTS restaurants (
                id SERIAL PRIMARY KEY,
                external_id VARCHAR(100),
                name VARCHAR(255) NOT NULL,
                address TEXT,
                city VARCHAR(100),
                region_code VARCHAR(50) REFERENCES regions(code),
                canton VARCHAR(50),
                country VARCHAR(100) DEFAULT 'Switzerland',
                postal_code VARCHAR(20),
                cuisine_type VARCHAR(100),
                cuisine_tags TEXT,
                price_range VARCHAR(20),
                price_level INTEGER,
                latitude REAL,
                longitude REAL,
                phone VARCHAR(50),
                email VARCHAR(255),
                contact_name VARCHAR(100),
                website TEXT,
                image_url TEXT,
                is_active INTEGER DEFAULT 1,
                first_listed_at TIMESTAMP,
                welcome_email_sent INTEGER DEFAULT 0,
                welcome_email_sent_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(name, address, city))""",
            
            # Reviews - Enhanced
            """CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                source VARCHAR(50),
                source_review_id VARCHAR(100),
                rating REAL,
                rating_max REAL DEFAULT 5.0,
                review_text TEXT,
                reviewer_name VARCHAR(100),
                review_date DATE,
                language VARCHAR(10) DEFAULT 'en',
                sentiment_score REAL,
                sentiment_label VARCHAR(20),
                text_polarity REAL,
                text_subjectivity REAL,
                aspect_food REAL,
                aspect_service REAL,
                aspect_ambiance REAL,
                aspect_value REAL,
                is_user_review INTEGER DEFAULT 0,
                user_id VARCHAR(100),
                verified INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW())""",
            
            # Source Ratings
            """CREATE TABLE IF NOT EXISTS source_ratings (
                id SERIAL PRIMARY KEY,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                source VARCHAR(50),
                avg_rating REAL,
                review_count INTEGER,
                data_quality_score REAL DEFAULT 1.0,
                last_updated TIMESTAMP DEFAULT NOW(),
                UNIQUE(restaurant_id, source))""",
            
            # Rankings - Per region
            """CREATE TABLE IF NOT EXISTS rankings (
                id SERIAL PRIMARY KEY,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                region_code VARCHAR(50) REFERENCES regions(code),
                composite_score REAL,
                sentiment_avg REAL,
                confidence_score REAL,
                data_quality_avg REAL,
                total_reviews INTEGER,
                user_reviews_count INTEGER DEFAULT 0,
                auto_rank INTEGER,
                manual_rank INTEGER,
                is_featured INTEGER DEFAULT 0,
                is_published INTEGER DEFAULT 0,
                is_pushed INTEGER DEFAULT 0,
                pushed_at TIMESTAMP,
                admin_notes TEXT,
                last_computed TIMESTAMP DEFAULT NOW(),
                UNIQUE(restaurant_id, region_code))""",
            
            # Scrape Jobs
            """CREATE TABLE IF NOT EXISTS scrape_jobs (
                id SERIAL PRIMARY KEY,
                job_type VARCHAR(50),
                status VARCHAR(20) DEFAULT 'pending',
                provider VARCHAR(50),
                region_code VARCHAR(50),
                location VARCHAR(100),
                min_rating REAL,
                max_price_level INTEGER,
                restaurants_found INTEGER DEFAULT 0,
                reviews_found INTEGER DEFAULT 0,
                errors TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW())""",
            
            # Custom Providers
            """CREATE TABLE IF NOT EXISTS custom_providers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                display_name VARCHAR(100),
                api_endpoint TEXT,
                auth_type VARCHAR(50) DEFAULT 'api_key',
                rate_limit INTEGER DEFAULT 100,
                cost_per_request REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW())""",
            
            # Ranking Weights
            """CREATE TABLE IF NOT EXISTS ranking_weights (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) DEFAULT 'default',
                is_active INTEGER DEFAULT 1,
                weight_google REAL DEFAULT 0.25,
                weight_yelp REAL DEFAULT 0.20,
                weight_tripadvisor REAL DEFAULT 0.25,
                weight_user_reviews REAL DEFAULT 0.15,
                weight_sentiment REAL DEFAULT 0.10,
                weight_text_sentiment REAL DEFAULT 0.50,
                weight_data_quality REAL DEFAULT 0.05,
                min_reviews_threshold INTEGER DEFAULT 5,
                recency_decay_days INTEGER DEFAULT 180,
                custom_weights TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
            
            # Custom Sentiment Keywords
            """CREATE TABLE IF NOT EXISTS sentiment_keywords (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(100) NOT NULL,
                category VARCHAR(50) NOT NULL,
                sentiment VARCHAR(20) NOT NULL,
                weight REAL DEFAULT 1.0,
                language VARCHAR(10) DEFAULT 'en',
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(keyword, category, language))""",
            
            # Website Config
            """CREATE TABLE IF NOT EXISTS website_config (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) DEFAULT 'default',
                api_url TEXT,
                api_key_encrypted TEXT,
                push_endpoint TEXT DEFAULT '/api/restaurants/import',
                auth_type VARCHAR(50) DEFAULT 'bearer',
                mapping_json TEXT,
                last_push TIMESTAMP,
                last_push_status VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
            
            # Push History - Enhanced with region
            """CREATE TABLE IF NOT EXISTS push_history (
                id SERIAL PRIMARY KEY,
                region_code VARCHAR(50),
                restaurants_pushed INTEGER,
                status VARCHAR(50),
                response_code INTEGER,
                response_message TEXT,
                pushed_at TIMESTAMP DEFAULT NOW())""",
            
            # Daily Tasks Log
            """CREATE TABLE IF NOT EXISTS daily_tasks (
                id SERIAL PRIMARY KEY,
                task_type VARCHAR(50),
                region_code VARCHAR(50),
                status VARCHAR(50),
                details TEXT,
                executed_at TIMESTAMP DEFAULT NOW())""",
            
            # Email Configuration
            """CREATE TABLE IF NOT EXISTS email_config (
                id SERIAL PRIMARY KEY,
                smtp_host VARCHAR(255) DEFAULT 'smtp.gmail.com',
                smtp_port INTEGER DEFAULT 587,
                smtp_user VARCHAR(255),
                smtp_password_encrypted TEXT,
                from_email VARCHAR(255),
                from_name VARCHAR(100) DEFAULT 'Spotwego',
                reply_to VARCHAR(255),
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
            
            # Email Templates
            """CREATE TABLE IF NOT EXISTS email_templates (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                subject VARCHAR(255),
                body_html TEXT,
                body_text TEXT,
                variables TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW())""",
            
            # Email Log
            """CREATE TABLE IF NOT EXISTS email_log (
                id SERIAL PRIMARY KEY,
                restaurant_id INTEGER REFERENCES restaurants(id),
                template_name VARCHAR(100),
                to_email VARCHAR(255),
                to_name VARCHAR(100),
                subject VARCHAR(255),
                status VARCHAR(50) DEFAULT 'pending',
                error_message TEXT,
                sent_at TIMESTAMP,
                opened_at TIMESTAMP,
                clicked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW())""",
            
            # Create indexes for performance
            """CREATE INDEX IF NOT EXISTS idx_restaurants_region ON restaurants(region_code)""",
            """CREATE INDEX IF NOT EXISTS idx_restaurants_city ON restaurants(city)""",
            """CREATE INDEX IF NOT EXISTS idx_restaurants_email ON restaurants(email)""",
            """CREATE INDEX IF NOT EXISTS idx_rankings_region ON rankings(region_code)""",
            """CREATE INDEX IF NOT EXISTS idx_rankings_published ON rankings(is_published)""",
            """CREATE INDEX IF NOT EXISTS idx_reviews_restaurant ON reviews(restaurant_id)""",
            """CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(is_user_review)""",
            """CREATE INDEX IF NOT EXISTS idx_email_log_restaurant ON email_log(restaurant_id)"""
        ]
    else:
        tables = [
            """CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT UNIQUE NOT NULL,
                api_key_encrypted TEXT,
                is_active INTEGER DEFAULT 1,
                provider_type TEXT DEFAULT 'builtin',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT,
                endpoint TEXT,
                request_count INTEGER DEFAULT 1,
                estimated_cost REAL DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT UNIQUE NOT NULL,
                name TEXT,
                country TEXT DEFAULT 'Switzerland',
                canton TEXT,
                latitude REAL,
                longitude REAL,
                zoom_level INTEGER DEFAULT 10,
                last_scan TIMESTAMP,
                restaurants_count INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                auto_push INTEGER DEFAULT 0,
                push_top_n INTEGER DEFAULT 20,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS restaurants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT,
                name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                region_code TEXT REFERENCES regions(code),
                canton TEXT,
                country TEXT DEFAULT 'Switzerland',
                postal_code TEXT,
                cuisine_type TEXT,
                cuisine_tags TEXT,
                price_range TEXT,
                price_level INTEGER,
                latitude REAL,
                longitude REAL,
                phone TEXT,
                email TEXT,
                contact_name TEXT,
                website TEXT,
                image_url TEXT,
                is_active INTEGER DEFAULT 1,
                first_listed_at TIMESTAMP,
                welcome_email_sent INTEGER DEFAULT 0,
                welcome_email_sent_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(name, address, city))""",
            
            """CREATE TABLE IF NOT EXISTS reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                source TEXT,
                source_review_id TEXT,
                rating REAL,
                rating_max REAL DEFAULT 5.0,
                review_text TEXT,
                reviewer_name TEXT,
                review_date DATE,
                language TEXT DEFAULT 'en',
                sentiment_score REAL,
                sentiment_label TEXT,
                text_polarity REAL,
                text_subjectivity REAL,
                aspect_food REAL,
                aspect_service REAL,
                aspect_ambiance REAL,
                aspect_value REAL,
                is_user_review INTEGER DEFAULT 0,
                user_id TEXT,
                verified INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS source_ratings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                source TEXT,
                avg_rating REAL,
                review_count INTEGER,
                data_quality_score REAL DEFAULT 1.0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(restaurant_id, source))""",
            
            """CREATE TABLE IF NOT EXISTS rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
                region_code TEXT REFERENCES regions(code),
                composite_score REAL,
                sentiment_avg REAL,
                confidence_score REAL,
                data_quality_avg REAL,
                total_reviews INTEGER,
                user_reviews_count INTEGER DEFAULT 0,
                auto_rank INTEGER,
                manual_rank INTEGER,
                is_featured INTEGER DEFAULT 0,
                is_published INTEGER DEFAULT 0,
                is_pushed INTEGER DEFAULT 0,
                pushed_at TIMESTAMP,
                admin_notes TEXT,
                last_computed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(restaurant_id, region_code))""",
            
            """CREATE TABLE IF NOT EXISTS scrape_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT,
                status TEXT DEFAULT 'pending',
                provider TEXT,
                region_code TEXT,
                location TEXT,
                min_rating REAL,
                max_price_level INTEGER,
                restaurants_found INTEGER DEFAULT 0,
                reviews_found INTEGER DEFAULT 0,
                errors TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS custom_providers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                display_name TEXT,
                api_endpoint TEXT,
                auth_type TEXT DEFAULT 'api_key',
                rate_limit INTEGER DEFAULT 100,
                cost_per_request REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS ranking_weights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT DEFAULT 'default',
                is_active INTEGER DEFAULT 1,
                weight_google REAL DEFAULT 0.25,
                weight_yelp REAL DEFAULT 0.20,
                weight_tripadvisor REAL DEFAULT 0.25,
                weight_user_reviews REAL DEFAULT 0.15,
                weight_sentiment REAL DEFAULT 0.10,
                weight_text_sentiment REAL DEFAULT 0.50,
                weight_data_quality REAL DEFAULT 0.05,
                min_reviews_threshold INTEGER DEFAULT 5,
                recency_decay_days INTEGER DEFAULT 180,
                custom_weights TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS sentiment_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                category TEXT NOT NULL,
                sentiment TEXT NOT NULL,
                weight REAL DEFAULT 1.0,
                language TEXT DEFAULT 'en',
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(keyword, category, language))""",
            
            """CREATE TABLE IF NOT EXISTS website_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT DEFAULT 'default',
                api_url TEXT,
                api_key_encrypted TEXT,
                push_endpoint TEXT DEFAULT '/api/restaurants/import',
                auth_type TEXT DEFAULT 'bearer',
                mapping_json TEXT,
                last_push TIMESTAMP,
                last_push_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS push_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region_code TEXT,
                restaurants_pushed INTEGER,
                status TEXT,
                response_code INTEGER,
                response_message TEXT,
                pushed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS daily_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT,
                region_code TEXT,
                status TEXT,
                details TEXT,
                executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS email_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                smtp_host TEXT DEFAULT 'smtp.gmail.com',
                smtp_port INTEGER DEFAULT 587,
                smtp_user TEXT,
                smtp_password_encrypted TEXT,
                from_email TEXT,
                from_name TEXT DEFAULT 'Spotwego',
                reply_to TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS email_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                subject TEXT,
                body_html TEXT,
                body_text TEXT,
                variables TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE TABLE IF NOT EXISTS email_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurant_id INTEGER REFERENCES restaurants(id),
                template_name TEXT,
                to_email TEXT,
                to_name TEXT,
                subject TEXT,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                sent_at TIMESTAMP,
                opened_at TIMESTAMP,
                clicked_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            
            """CREATE INDEX IF NOT EXISTS idx_restaurants_region ON restaurants(region_code)""",
            """CREATE INDEX IF NOT EXISTS idx_restaurants_city ON restaurants(city)""",
            """CREATE INDEX IF NOT EXISTS idx_restaurants_email ON restaurants(email)""",
            """CREATE INDEX IF NOT EXISTS idx_rankings_region ON rankings(region_code)""",
            """CREATE INDEX IF NOT EXISTS idx_rankings_published ON rankings(is_published)""",
            """CREATE INDEX IF NOT EXISTS idx_reviews_restaurant ON reviews(restaurant_id)""",
            """CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(is_user_review)""",
            """CREATE INDEX IF NOT EXISTS idx_email_log_restaurant ON email_log(restaurant_id)"""
        ]
    
    for sql in tables:
        try:
            cur.execute(sql)
        except Exception as e:
            print(f"Table/index warning: {e}")
    
    # Insert default weights
    try:
        if USE_POSTGRES:
            cur.execute("INSERT INTO ranking_weights (name) VALUES ('default') ON CONFLICT DO NOTHING")
        else:
            cur.execute("INSERT OR IGNORE INTO ranking_weights (name) VALUES ('default')")
    except:
        pass
    
    # ========== MIGRATIONS: Create missing tables and add missing columns ==========
    
    # First, create any missing tables (for databases created before these tables existed)
    missing_tables_postgres = [
        """CREATE TABLE IF NOT EXISTS sentiment_keywords (
            id SERIAL PRIMARY KEY,
            keyword VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            sentiment VARCHAR(20) NOT NULL,
            weight REAL DEFAULT 1.0,
            language VARCHAR(10) DEFAULT 'en',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(keyword, category, language))""",
        """CREATE TABLE IF NOT EXISTS email_log (
            id SERIAL PRIMARY KEY,
            restaurant_id INTEGER REFERENCES restaurants(id),
            template_name VARCHAR(100),
            to_email VARCHAR(255),
            to_name VARCHAR(255),
            subject VARCHAR(500),
            status VARCHAR(50) DEFAULT 'pending',
            error_message TEXT,
            sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW())""",
    ]
    
    missing_tables_sqlite = [
        """CREATE TABLE IF NOT EXISTS sentiment_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword TEXT NOT NULL,
            category TEXT NOT NULL,
            sentiment TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            language TEXT DEFAULT 'en',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(keyword, category, language))""",
        """CREATE TABLE IF NOT EXISTS email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER,
            template_name TEXT,
            to_email TEXT,
            to_name TEXT,
            subject TEXT,
            status TEXT DEFAULT 'pending',
            error_message TEXT,
            sent_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    ]
    
    tables_to_create = missing_tables_postgres if USE_POSTGRES else missing_tables_sqlite
    for sql in tables_to_create:
        try:
            cur.execute(sql)
            conn.commit()
            print(f"Migration: Created missing table")
        except Exception as e:
            print(f"Table creation note: {e}")
    
    # Now add missing columns
    migrations = []
    
    if USE_POSTGRES:
        migrations = [
            # Reviews table migrations
            ("reviews", "is_user_review", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS is_user_review INTEGER DEFAULT 0"),
            ("reviews", "text_polarity", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS text_polarity REAL"),
            ("reviews", "text_subjectivity", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS text_subjectivity REAL"),
            ("reviews", "aspect_food", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS aspect_food REAL"),
            ("reviews", "aspect_service", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS aspect_service REAL"),
            ("reviews", "aspect_ambiance", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS aspect_ambiance REAL"),
            ("reviews", "aspect_value", "ALTER TABLE reviews ADD COLUMN IF NOT EXISTS aspect_value REAL"),
            # Restaurants table migrations
            ("restaurants", "email", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS email VARCHAR(255)"),
            ("restaurants", "phone", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS phone VARCHAR(50)"),
            ("restaurants", "website", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS website TEXT"),
            ("restaurants", "google_place_id", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS google_place_id VARCHAR(255)"),
            ("restaurants", "is_excluded", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS is_excluded INTEGER DEFAULT 0"),
            ("restaurants", "exclude_reason", "ALTER TABLE restaurants ADD COLUMN IF NOT EXISTS exclude_reason VARCHAR(255)"),
            # Ranking weights migrations
            ("ranking_weights", "weight_text_sentiment", "ALTER TABLE ranking_weights ADD COLUMN IF NOT EXISTS weight_text_sentiment REAL DEFAULT 0.50"),
        ]
    else:
        # SQLite doesn't have IF NOT EXISTS for columns, so we check first
        def sqlite_add_column(table, column, col_type, default=None):
            try:
                cur.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except:
                default_clause = f" DEFAULT {default}" if default is not None else ""
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}{default_clause}")
        
        sqlite_migrations = [
            ("reviews", "is_user_review", "INTEGER", 0),
            ("reviews", "text_polarity", "REAL", None),
            ("reviews", "text_subjectivity", "REAL", None),
            ("reviews", "aspect_food", "REAL", None),
            ("reviews", "aspect_service", "REAL", None),
            ("reviews", "aspect_ambiance", "REAL", None),
            ("reviews", "aspect_value", "REAL", None),
            ("restaurants", "email", "TEXT", None),
            ("restaurants", "phone", "TEXT", None),
            ("restaurants", "website", "TEXT", None),
            ("restaurants", "google_place_id", "TEXT", None),
            ("restaurants", "is_excluded", "INTEGER", 0),
            ("restaurants", "exclude_reason", "TEXT", None),
            ("ranking_weights", "weight_text_sentiment", "REAL", 0.50),
        ]
        for table, col, col_type, default in sqlite_migrations:
            try:
                sqlite_add_column(table, col, col_type, default)
            except Exception as e:
                print(f"Migration warning ({table}.{col}): {e}")
    
    for table, col, sql in migrations:
        try:
            cur.execute(sql)
            print(f"Migration: Added {col} to {table}")
        except Exception as e:
            # Column might already exist or other expected errors
            pass
    
    conn.commit()
    cur.close()
    conn.close()


def load_config():
    if os.path.exists(Config.CONFIG_PATH):
        with open(Config.CONFIG_PATH) as f:
            return json.load(f)
    return {'api_keys': {}, 'website': {}}


def save_config(cfg):
    os.makedirs(os.path.dirname(Config.CONFIG_PATH) or '.', exist_ok=True)
    with open(Config.CONFIG_PATH, 'w') as f:
        json.dump(cfg, f)


def dt_to_str(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def dict_row(row):
    if row is None:
        return None
    return dict(row)


def generate_mock_restaurants(location, count=100):
    """Generate mock restaurant data with emails"""
    
    cuisines = [
        ("French Gastronomic", "$$$$", 4), ("French Bistro", "$$$", 3), ("French Brasserie", "$$", 2),
        ("Italian Fine Dining", "$$$$", 4), ("Italian Trattoria", "$$$", 3), ("Pizzeria", "$$", 2),
        ("Swiss Traditional", "$$$", 3), ("Swiss Fondue", "$$$", 3), ("Swiss Raclette", "$$", 2),
        ("Japanese Omakase", "$$$$", 4), ("Japanese Sushi", "$$$", 3), ("Japanese Ramen", "$$", 2),
        ("Chinese Fine Dining", "$$$", 3), ("Chinese Cantonese", "$$", 2), ("Chinese Dim Sum", "$$", 2),
        ("Thai", "$$", 2), ("Vietnamese", "$$", 2), ("Indian", "$$", 2), ("Lebanese", "$$", 2),
        ("Mexican", "$$", 2), ("Peruvian", "$$$", 3), ("Brazilian", "$$$", 3),
        ("Spanish Tapas", "$$$", 3), ("Greek", "$$", 2), ("Turkish", "$$", 2),
        ("American Steakhouse", "$$$$", 4), ("Burgers", "$", 1), ("BBQ", "$$", 2),
        ("Seafood", "$$$", 3), ("Vegetarian", "$$", 2), ("Vegan", "$$", 2),
        ("Fusion", "$$$", 3), ("Mediterranean", "$$$", 3), ("Modern European", "$$$$", 4),
    ]
    
    prefixes = ["Le", "La", "Chez", "Restaurant", "Café", "Brasserie", "Auberge", "Bistro", "Osteria", "Trattoria"]
    names_part1 = ["Petit", "Grand", "Vieux", "Nouveau", "Beau", "Bon", "Joli", "Royal", "Imperial", "Golden"]
    names_part2 = ["Lion", "Soleil", "Lune", "Étoile", "Rose", "Jardin", "Lac", "Mont", "Chat", "Coq",
                   "Cerf", "Ours", "Aigle", "Cygne", "Paon", "Phoenix", "Dragon", "Lotus", "Bambou", "Olivier"]
    
    first_names = ["Jean", "Pierre", "Marie", "Sophie", "Marc", "Paul", "Anna", "Lucas", "Emma", "Thomas",
                   "Julie", "Antoine", "Claire", "Nicolas", "Laura", "David", "Sarah", "Michel", "Isabelle", "François"]
    last_names = ["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Hoffmann", "Schäfer",
                  "Dubois", "Martin", "Bernard", "Petit", "Robert", "Richard", "Durand", "Leroy", "Moreau", "Simon"]
    
    streets = ["Rue de la Gare", "Avenue de la Paix", "Rue du Lac", "Boulevard des Alpes", "Rue Centrale",
               "Place du Marché", "Rue de l'Église", "Avenue des Fleurs", "Rue du Commerce", "Chemin des Vignes",
               "Rue de Lausanne", "Avenue de Genève", "Rue du Rhône", "Quai des Bergues", "Rue de Berne"]
    
    restaurants = []
    used_names = set()
    
    for i in range(count):
        # Generate unique name
        attempts = 0
        while attempts < 50:
            if random.random() < 0.3:
                name = f"{random.choice(prefixes)} {random.choice(names_part2)}"
            elif random.random() < 0.5:
                name = f"{random.choice(prefixes)} {random.choice(names_part1)} {random.choice(names_part2)}"
            else:
                name = f"Chez {random.choice(first_names)}"
            
            if name not in used_names:
                used_names.add(name)
                break
            attempts += 1
        
        if attempts >= 50:
            name = f"Restaurant {location} #{i+1}"
        
        cuisine, price_range, price_level = random.choice(cuisines)
        base_rating = round(random.uniform(3.2, 4.9), 1)
        
        # Generate contact info
        contact_first = random.choice(first_names)
        contact_last = random.choice(last_names)
        contact_name = f"{contact_first} {contact_last}"
        
        # Generate email (80% have email)
        email = None
        if random.random() < 0.8:
            email_name = name.lower().replace(" ", "").replace("'", "").replace("é", "e").replace("è", "e").replace("ê", "e").replace("ô", "o").replace("î", "i")[:20]
            email = f"{email_name}@restaurant-{location.lower()}.ch"
        
        # Generate phone
        phone = f"+41 {random.randint(21, 79)} {random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"
        
        # Generate address
        street = random.choice(streets)
        addr = f"{street} {random.randint(1, 150)}"
        postal = f"{random.randint(1200, 1299)}" if location == "Geneva" else f"{random.randint(1000, 9999)}"
        
        restaurants.append({
            'name': name,
            'cuisine': cuisine,
            'price_range': price_range,
            'price_level': price_level,
            'base_rating': base_rating,
            'address': addr,
            'postal_code': postal,
            'phone': phone,
            'email': email,
            'contact_name': contact_name,
            'website': f"https://www.{name.lower().replace(' ', '-').replace('é', 'e')[:20]}.ch" if random.random() < 0.6 else None
        })
    
    return restaurants


def extract_email_from_website(website_url, timeout=5):
    """
    Extract email address from a restaurant website.
    Checks main page, contact page, and about page.
    Returns first valid email found or None.
    """
    import requests
    import re
    from urllib.parse import urljoin, urlparse
    
    if not website_url:
        return None
    
    # Normalize URL
    if not website_url.startswith('http'):
        website_url = 'https://' + website_url
    
    # Email regex pattern - matches most valid emails
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    
    # Common email patterns to exclude (generic/spam traps)
    exclude_patterns = [
        r'example\.com', r'domain\.com', r'email\.com', r'test\.com',
        r'wordpress', r'wix', r'squarespace', r'godaddy',
        r'sentry\.io', r'github\.com', r'google\.com',
        r'placeholder', r'your-?email', r'name@'
    ]
    
    # Pages to check for contact info
    contact_paths = ['', '/contact', '/kontakt', '/contact-us', '/about', '/impressum', '/a-propos', '/chi-siamo']
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5'
    }
    
    found_emails = set()
    
    for path in contact_paths:
        try:
            url = urljoin(website_url.rstrip('/') + '/', path.lstrip('/'))
            
            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            
            if response.status_code == 200:
                content = response.text
                
                # Find all email addresses
                emails = re.findall(email_pattern, content, re.IGNORECASE)
                
                for email in emails:
                    email = email.lower().strip()
                    
                    # Skip excluded patterns
                    if any(re.search(exc, email, re.IGNORECASE) for exc in exclude_patterns):
                        continue
                    
                    # Skip very long emails (likely false positives)
                    if len(email) > 50:
                        continue
                    
                    # Skip emails that look like image filenames
                    if email.endswith(('.png', '.jpg', '.gif', '.svg', '.webp')):
                        continue
                    
                    found_emails.add(email)
                
                # If we found emails on this page, prioritize by domain match
                if found_emails:
                    domain = urlparse(website_url).netloc.replace('www.', '')
                    
                    # Prefer emails from the same domain
                    for email in found_emails:
                        if domain.split('.')[0] in email:
                            return email
                    
                    # Prefer info@, contact@, hello@, reservation@
                    priority_prefixes = ['info', 'contact', 'hello', 'reservation', 'reservations', 'book', 'booking']
                    for prefix in priority_prefixes:
                        for email in found_emails:
                            if email.startswith(prefix + '@'):
                                return email
                    
                    # Return first email found
                    return list(found_emails)[0]
                    
        except requests.exceptions.Timeout:
            continue
        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue
    
    return None


def extract_emails_batch(restaurants, max_concurrent=5):
    """
    Extract emails for a batch of restaurants.
    Returns dict of {place_id: email}
    """
    import concurrent.futures
    
    results = {}
    
    def fetch_email(restaurant):
        place_id = restaurant.get('place_id') or restaurant.get('google_place_id')
        website = restaurant.get('website')
        if website:
            email = extract_email_from_website(website)
            return (place_id, email)
        return (place_id, None)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {executor.submit(fetch_email, r): r for r in restaurants if r.get('website')}
        
        for future in concurrent.futures.as_completed(futures):
            try:
                place_id, email = future.result()
                if email:
                    results[place_id] = email
            except Exception:
                pass
    
    return results


def analyze_review_sentiment(review_text, language='en', custom_keywords=None):
    """
    Analyze sentiment of review text using NLP.
    Returns dict with polarity, subjectivity, label, and aspect scores.
    
    Polarity: -1 (negative) to +1 (positive)
    Subjectivity: 0 (objective) to 1 (subjective)
    """
    if not review_text or len(review_text.strip()) < 3:
        return {
            'polarity': 0,
            'subjectivity': 0,
            'label': 'neutral',
            'confidence': 0,
            'aspects': {}
        }
    
    # Load custom keywords from DB if not provided
    if custom_keywords is None:
        try:
            custom_keywords = get_custom_keywords_from_db()
        except:
            custom_keywords = {}
    
    try:
        from textblob import TextBlob
        HAS_TEXTBLOB = True
    except ImportError:
        HAS_TEXTBLOB = False
    
    if not HAS_TEXTBLOB:
        # Fallback to simple keyword-based analysis
        return analyze_sentiment_keywords(review_text, custom_keywords)
    
    try:
        blob = TextBlob(review_text)
        
        # Get overall sentiment
        polarity = blob.sentiment.polarity  # -1 to 1
        subjectivity = blob.sentiment.subjectivity  # 0 to 1
        
        # Determine label
        if polarity > 0.2:
            label = 'positive'
        elif polarity < -0.2:
            label = 'negative'
        else:
            label = 'neutral'
        
        # Confidence based on subjectivity and text length
        text_length_factor = min(len(review_text) / 200, 1)  # More text = more confident
        confidence = (subjectivity * 0.5 + text_length_factor * 0.5)
        
        # Aspect-based sentiment (restaurant-specific) with custom keywords
        aspects = analyze_aspects(review_text, blob, custom_keywords)
        
        return {
            'polarity': round(polarity, 3),
            'subjectivity': round(subjectivity, 3),
            'label': label,
            'confidence': round(confidence, 3),
            'aspects': aspects
        }
        
    except Exception as e:
        # Fallback on error
        return analyze_sentiment_keywords(review_text, custom_keywords)


def analyze_aspects(review_text, blob=None, custom_keywords=None):
    """
    Analyze sentiment for specific restaurant aspects.
    Returns dict of aspect -> score (-1 to 1)
    
    custom_keywords: dict of {category: {positive: [...], negative: [...]}}
    """
    text_lower = review_text.lower()
    
    # Default aspect keywords with sentiment indicators
    aspect_keywords = {
        'food': {
            'positive': ['delicious', 'tasty', 'fresh', 'flavorful', 'amazing food', 'excellent food', 
                        'best food', 'great food', 'good food', 'yummy', 'scrumptious', 'mouth-watering',
                        'perfectly cooked', 'well seasoned', 'authentic', 'homemade'],
            'negative': ['bland', 'tasteless', 'cold', 'undercooked', 'overcooked', 'stale', 'frozen',
                        'bad food', 'terrible food', 'disgusting', 'inedible', 'raw', 'burnt', 'soggy',
                        'too salty', 'too sweet', 'greasy', 'dry']
        },
        'service': {
            'positive': ['friendly', 'attentive', 'helpful', 'professional', 'fast service', 'great service',
                        'excellent service', 'wonderful staff', 'polite', 'welcoming', 'accommodating',
                        'quick service', 'efficient', 'knowledgeable'],
            'negative': ['slow', 'rude', 'ignored', 'unfriendly', 'bad service', 'terrible service',
                        'poor service', 'waited forever', 'inattentive', 'dismissive', 'arrogant',
                        'unprofessional', 'forgotten', 'mistake', 'wrong order']
        },
        'ambiance': {
            'positive': ['cozy', 'romantic', 'beautiful', 'lovely atmosphere', 'great ambiance', 'clean',
                        'nice decor', 'comfortable', 'charming', 'elegant', 'modern', 'stylish',
                        'great view', 'peaceful', 'relaxing'],
            'negative': ['noisy', 'loud', 'dirty', 'cramped', 'smelly', 'uncomfortable', 'dark',
                        'crowded', 'chaotic', 'run-down', 'dated', 'stuffy', 'cold', 'hot']
        },
        'value': {
            'positive': ['worth it', 'good value', 'reasonable', 'affordable', 'generous portions',
                        'great price', 'cheap', 'bargain', 'bang for buck', 'fair price'],
            'negative': ['overpriced', 'expensive', 'rip-off', 'tiny portions', 'small portions',
                        'not worth', 'too expensive', 'highway robbery', 'poor value']
        }
    }
    
    # Merge custom keywords if provided
    if custom_keywords:
        for category, sentiments in custom_keywords.items():
            if category not in aspect_keywords:
                aspect_keywords[category] = {'positive': [], 'negative': []}
            for sentiment, keywords in sentiments.items():
                if sentiment in aspect_keywords[category]:
                    # Add custom keywords (avoid duplicates)
                    existing = set(aspect_keywords[category][sentiment])
                    for kw in keywords:
                        if kw.lower() not in existing:
                            aspect_keywords[category][sentiment].append(kw.lower())
    
    aspects = {}
    
    for aspect, keywords in aspect_keywords.items():
        positive_count = sum(1 for kw in keywords['positive'] if kw in text_lower)
        negative_count = sum(1 for kw in keywords['negative'] if kw in text_lower)
        
        if positive_count > 0 or negative_count > 0:
            total = positive_count + negative_count
            score = (positive_count - negative_count) / total
            aspects[aspect] = round(score, 2)
    
    return aspects


def get_custom_keywords_from_db():
    """
    Load custom sentiment keywords from database.
    Returns dict of {category: {positive: [...], negative: [...]}}
    """
    try:
        conn = get_db()
        cur = get_cursor(conn)
        
        cur.execute("""SELECT keyword, category, sentiment FROM sentiment_keywords 
            WHERE is_active=1 ORDER BY category, sentiment""")
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        custom = {}
        for row in rows:
            r = dict_row(row) if hasattr(row, 'keys') else {'keyword': row[0], 'category': row[1], 'sentiment': row[2]}
            category = r['category'].lower()
            sentiment = r['sentiment'].lower()
            keyword = r['keyword'].lower()
            
            if category not in custom:
                custom[category] = {'positive': [], 'negative': []}
            
            if sentiment in custom[category]:
                custom[category][sentiment].append(keyword)
        
        return custom
    except Exception as e:
        return {}


def analyze_sentiment_keywords(review_text, custom_keywords=None):
    """
    Fallback keyword-based sentiment analysis when TextBlob is not available.
    """
    text_lower = review_text.lower()
    
    positive_words = [
        'amazing', 'excellent', 'great', 'wonderful', 'fantastic', 'perfect', 'love', 'loved',
        'best', 'delicious', 'fresh', 'friendly', 'recommend', 'outstanding', 'superb',
        'incredible', 'awesome', 'brilliant', 'fabulous', 'exceptional', 'impressive'
    ]
    
    negative_words = [
        'terrible', 'awful', 'horrible', 'bad', 'worst', 'disappointing', 'poor', 'mediocre',
        'disgusting', 'rude', 'slow', 'cold', 'overpriced', 'never', 'avoid', 'waste',
        'tasteless', 'bland', 'dirty', 'uncomfortable', 'unprofessional'
    ]
    
    # Add custom keywords from all categories
    if custom_keywords:
        for category, sentiments in custom_keywords.items():
            if 'positive' in sentiments:
                positive_words.extend([kw.lower() for kw in sentiments['positive']])
            if 'negative' in sentiments:
                negative_words.extend([kw.lower() for kw in sentiments['negative']])
    
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    total = positive_count + negative_count
    
    if total == 0:
        return {
            'polarity': 0,
            'subjectivity': 0.5,
            'label': 'neutral',
            'confidence': 0.3,
            'aspects': {}
        }
    
    polarity = (positive_count - negative_count) / max(total, 1)
    polarity = max(-1, min(1, polarity))  # Clamp to -1, 1
    
    if polarity > 0.2:
        label = 'positive'
    elif polarity < -0.2:
        label = 'negative'
    else:
        label = 'neutral'
    
    return {
        'polarity': round(polarity, 3),
        'subjectivity': 0.7,
        'label': label,
        'confidence': min(total / 5, 1),
        'aspects': analyze_aspects(review_text, custom_keywords=custom_keywords)
    }


def combine_rating_and_text_sentiment(star_rating, text_sentiment, text_weight=0.5):
    """
    Combine star rating with text sentiment analysis.
    
    star_rating: 1-5 scale
    text_sentiment: dict from analyze_review_sentiment()
    text_weight: 0-1, how much to weight text vs stars (0.5 = equal weight)
    
    Returns combined sentiment score (-1 to 1)
    """
    # Convert star rating to -1 to 1 scale
    star_sentiment = (star_rating - 3) / 2  # 1=-1, 3=0, 5=1
    
    # Get text polarity
    text_polarity = text_sentiment.get('polarity', 0)
    text_confidence = text_sentiment.get('confidence', 0)
    
    # If no text or low confidence, rely more on stars
    if text_confidence < 0.3:
        effective_text_weight = text_weight * text_confidence
    else:
        effective_text_weight = text_weight
    
    star_weight = 1 - effective_text_weight
    
    # Combine
    combined = (star_sentiment * star_weight) + (text_polarity * effective_text_weight)
    
    return round(combined, 3)


def generate_mock_search_results(city, country, radius_km=5, min_rating=0, min_reviews=0):
    """Generate realistic mock restaurant search results for testing"""
    
    # Restaurant name components by country
    name_styles = {
        'Switzerland': {
            'prefixes': ['Le', 'La', 'Chez', 'Restaurant', 'Café', 'Brasserie', 'Auberge'],
            'names': ['Soleil', 'Lac', 'Paix', 'Étoile', 'Montagne', 'Vieux Pont', 'Belle Vue', 'Château']
        },
        'France': {
            'prefixes': ['Le', 'La', 'Chez', 'Bistro', 'Brasserie', 'Au'],
            'names': ['Petit Chou', 'Coq d\'Or', 'Bon Vivant', 'Tour Eiffel', 'Marché', 'Jardin', 'Moulin']
        },
        'Germany': {
            'prefixes': ['Gasthaus', 'Restaurant', 'Zum', 'Zur', 'Das'],
            'names': ['Adler', 'Löwen', 'Stern', 'Krone', 'Hirsch', 'Ratshaus', 'Altes Haus']
        },
        'Italy': {
            'prefixes': ['Ristorante', 'Trattoria', 'Osteria', 'Pizzeria', 'Da'],
            'names': ['Roma', 'Napoli', 'Bella Italia', 'Sole', 'Luna', 'Giovanni', 'Mario']
        },
        'UK': {
            'prefixes': ['The', ''],
            'names': ['Crown', 'Rose & Crown', 'Red Lion', 'White Hart', 'Kings Arms', 'Golden Fleece']
        },
        'USA': {
            'prefixes': ['The', ''],
            'names': ['Corner Bistro', 'Main Street Grill', 'Downtown Kitchen', 'Urban Eatery', 'City Lights']
        },
        'Spain': {
            'prefixes': ['El', 'La', 'Casa', 'Mesón', 'Taberna'],
            'names': ['Sol', 'Toro', 'Olivo', 'Mar', 'Jardín', 'Bodega']
        }
    }
    
    cuisines = ['Italian', 'French', 'Japanese', 'Chinese', 'Thai', 'Indian', 'Mexican', 
                'Mediterranean', 'Seafood', 'Steakhouse', 'Vegetarian', 'Modern European', 'Local']
    
    style = name_styles.get(country, name_styles['France'])
    results = []
    
    # Generate 30-80 mock results
    count = random.randint(30, 80)
    used_names = set()
    
    for i in range(count):
        # Generate unique name
        for _ in range(10):
            prefix = random.choice(style['prefixes'])
            base = random.choice(style['names'])
            name = f"{prefix} {base}".strip() if prefix else base
            if name not in used_names:
                used_names.add(name)
                break
        else:
            name = f"Restaurant {city} #{i+1}"
        
        # Generate realistic rating (mostly 3.5-4.8)
        rating = round(random.gauss(4.2, 0.5), 1)
        rating = max(2.5, min(5.0, rating))
        
        # Generate review count (power law distribution)
        review_count = int(random.paretovariate(1.5) * 20)
        review_count = min(review_count, 2000)
        
        # Apply filters
        if rating < min_rating or review_count < min_reviews:
            continue
        
        # Random location within radius
        lat_offset = random.uniform(-radius_km/111, radius_km/111)
        lng_offset = random.uniform(-radius_km/111, radius_km/111)
        
        results.append({
            'place_id': f"mock_{city}_{i}",
            'name': name,
            'address': f"{random.randint(1, 200)} {random.choice(['Main St', 'High St', 'Market St', 'Church Rd', 'Park Ave'])}, {city}",
            'rating': rating,
            'review_count': review_count,
            'price_level': random.choices([1, 2, 3, 4], weights=[15, 40, 35, 10])[0],
            'latitude': lat_offset,  # Will be relative to city center
            'longitude': lng_offset,
            'types': [random.choice(['restaurant', 'cafe', 'bar']), f"{random.choice(cuisines).lower()}_restaurant"],
            'is_open': random.choice([True, False, None])
        })
    
    return results


def send_welcome_email(restaurant_id, conn=None):
    """Send welcome email to a restaurant when first listed"""
    should_close = False
    if conn is None:
        conn = get_db()
        should_close = True
    
    cur = get_cursor(conn)
    p = '%s' if USE_POSTGRES else '?'
    
    try:
        # Get restaurant info
        cur.execute(f"SELECT * FROM restaurants WHERE id={p}", (restaurant_id,))
        rest = cur.fetchone()
        if not rest:
            return {'success': False, 'error': 'Restaurant not found'}
        
        rest = dict_row(rest)
        
        if not rest.get('email'):
            return {'success': False, 'error': 'No email address'}
        
        if rest.get('welcome_email_sent'):
            return {'success': False, 'error': 'Already sent'}
        
        # Get email config
        cur.execute("SELECT * FROM email_config WHERE is_active=1 LIMIT 1")
        email_cfg = cur.fetchone()
        if not email_cfg:
            return {'success': False, 'error': 'Email not configured'}
        
        email_cfg = dict_row(email_cfg)
        
        # Get template
        cur.execute(f"SELECT * FROM email_templates WHERE name='welcome' AND is_active=1 LIMIT 1")
        template = cur.fetchone()
        
        if template:
            template = dict_row(template)
            subject = template['subject']
            body_html = template['body_html']
        else:
            # Default template with Spotwego branding
            subject = "Congratulations! Your restaurant is now featured on Spotwego"
            body_html = """
            <html>
            <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: #6B4444; padding: 20px; text-align: center;">
                    <h1 style="color: white; margin: 0; font-family: Georgia, serif; font-style: italic;">Spotwego</h1>
                </div>
                <div style="padding: 30px; background: #FAF7F5;">
                    <h2 style="color: #3D2929;">Congratulations, {{contact_name}}!</h2>
                    <p style="color: #3D2929;">We're excited to inform you that <strong>{{restaurant_name}}</strong> has been selected to be featured on Spotwego!</p>
                    <p style="color: #3D2929;">Your restaurant was chosen based on:</p>
                    <ul style="color: #6B4444;">
                        <li>Excellent customer reviews</li>
                        <li>Quality of service</li>
                        <li>Positive sentiment analysis</li>
                    </ul>
                    <p style="color: #3D2929;">This means increased visibility to food enthusiasts looking for the best dining experiences in {{city}}.</p>
                    <p style="color: #3D2929;"><strong>What's next?</strong></p>
                    <p style="color: #3D2929;">No action is required from you. Your listing is now live and will be seen by thousands of potential customers.</p>
                    <p style="color: #3D2929;">If you'd like to update your information or have any questions, simply reply to this email.</p>
                    <br>
                    <p style="color: #3D2929;">Best regards,<br><strong>The Spotwego Team</strong></p>
                </div>
                <div style="background: #6B4444; padding: 15px; text-align: center; color: #C4A4A4; font-size: 12px;">
                    © 2025 Spotwego. All rights reserved.
                </div>
            </body>
            </html>
            """
        
        # Replace variables
        subject = subject.replace('{{restaurant_name}}', rest.get('name', ''))
        body_html = body_html.replace('{{restaurant_name}}', rest.get('name', ''))
        body_html = body_html.replace('{{contact_name}}', rest.get('contact_name', 'Restaurant Owner'))
        body_html = body_html.replace('{{city}}', rest.get('city', ''))
        body_html = body_html.replace('{{address}}', rest.get('address', ''))
        
        # Try to send email
        cfg = load_config()
        smtp_password = cfg.get('email', {}).get('smtp_password', '')
        
        if smtp_password and email_cfg.get('smtp_user'):
            try:
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart
                
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = f"{email_cfg.get('from_name', 'Spotwego')} <{email_cfg.get('from_email', email_cfg['smtp_user'])}>"
                msg['To'] = rest['email']
                
                if email_cfg.get('reply_to'):
                    msg['Reply-To'] = email_cfg['reply_to']
                
                msg.attach(MIMEText(body_html, 'html'))
                
                server = smtplib.SMTP(email_cfg.get('smtp_host', 'smtp.gmail.com'), email_cfg.get('smtp_port', 587))
                server.starttls()
                server.login(email_cfg['smtp_user'], smtp_password)
                server.send_message(msg)
                server.quit()
                
                status = 'sent'
            except Exception as e:
                status = 'failed'
                # Log the email anyway
                if USE_POSTGRES:
                    cur.execute("""INSERT INTO email_log (restaurant_id, template_name, to_email, to_name, subject, status, error_message)
                        VALUES (%s, 'welcome', %s, %s, %s, %s, %s)""",
                        (restaurant_id, rest['email'], rest.get('contact_name'), subject, status, str(e)))
                else:
                    cur.execute("""INSERT INTO email_log (restaurant_id, template_name, to_email, to_name, subject, status, error_message)
                        VALUES (?, 'welcome', ?, ?, ?, ?, ?)""",
                        (restaurant_id, rest['email'], rest.get('contact_name'), subject, status, str(e)))
                conn.commit()
                return {'success': False, 'error': str(e)}
        else:
            status = 'queued'  # No SMTP configured, just queue it
        
        # Log email
        if USE_POSTGRES:
            cur.execute("""INSERT INTO email_log (restaurant_id, template_name, to_email, to_name, subject, status, sent_at)
                VALUES (%s, 'welcome', %s, %s, %s, %s, NOW())""",
                (restaurant_id, rest['email'], rest.get('contact_name'), subject, status))
            cur.execute("UPDATE restaurants SET welcome_email_sent=1, welcome_email_sent_at=NOW(), first_listed_at=NOW() WHERE id=%s", (restaurant_id,))
        else:
            cur.execute("""INSERT INTO email_log (restaurant_id, template_name, to_email, to_name, subject, status, sent_at)
                VALUES (?, 'welcome', ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                (restaurant_id, rest['email'], rest.get('contact_name'), subject, status))
            cur.execute("UPDATE restaurants SET welcome_email_sent=1, welcome_email_sent_at=CURRENT_TIMESTAMP, first_listed_at=CURRENT_TIMESTAMP WHERE id=?", (restaurant_id,))
        
        conn.commit()
        return {'success': True, 'status': status, 'email': rest['email']}
        
    finally:
        cur.close()
        if should_close:
            conn.close()


# ============================================================================
# ROUTES
# ============================================================================

def register_routes(app):
    
    @app.route('/health')
    def health():
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute('SELECT 1')
            cur.close()
            conn.close()
            return jsonify({'status': 'healthy', 'db': 'PostgreSQL' if USE_POSTGRES else 'SQLite'})
        except Exception as e:
            return jsonify({'status': 'error', 'msg': str(e)}), 500
    
    @app.route('/')
    def index():
        # Use v=1 to access old dashboard, default is new dashboard
        version = request.args.get('v', '2')
        if version == '1':
            return render_template('dashboard.html')
        return render_template('dashboard_v2.html')
    
    @app.route('/old')
    def old_dashboard():
        """Access old dashboard directly"""
        return render_template('dashboard.html')
    
    # ========== STATS ==========
    
    @app.route('/api/stats')
    def stats():
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            def fetch_one(sql):
                try:
                    cur.execute(sql)
                    row = cur.fetchone()
                    if row is None:
                        return 0
                    if hasattr(row, 'keys'):
                        return row.get('c', 0) or 0
                    return row[0] if row else 0
                except Exception as e:
                    app.logger.warning(f"Stats query failed: {sql[:50]}... - {e}")
                    # PostgreSQL requires rollback after error
                    if USE_POSTGRES:
                        try:
                            conn.rollback()
                        except:
                            pass
                    return 0
            
            rest_count = fetch_one("SELECT COUNT(*) as c FROM restaurants WHERE is_active=1")
            rev_count = fetch_one("SELECT COUNT(*) as c FROM reviews")
            # is_user_review column might not exist in older databases
            user_rev = fetch_one("SELECT COUNT(*) as c FROM reviews WHERE is_user_review=1")
            pub_count = fetch_one("SELECT COUNT(*) as c FROM rankings WHERE is_published=1")
            pushed = fetch_one("SELECT COUNT(*) as c FROM rankings WHERE is_pushed=1")
            regions = fetch_one("SELECT COUNT(*) as c FROM regions")
            
            # Monthly cost
            try:
                if USE_POSTGRES:
                    cur.execute("SELECT COALESCE(SUM(estimated_cost),0) as c FROM api_usage WHERE timestamp >= date_trunc('month', NOW())")
                    row = cur.fetchone()
                    cost = row.get('c', 0) if hasattr(row, 'keys') else (row[0] if row else 0)
                else:
                    cur.execute("SELECT COALESCE(SUM(estimated_cost),0) as c FROM api_usage WHERE timestamp >= date('now','start of month')")
                    row = cur.fetchone()
                    cost = row[0] if row else 0
            except Exception as e:
                app.logger.warning(f"Cost query failed: {e}")
                if USE_POSTGRES:
                    try:
                        conn.rollback()
                    except:
                        pass
                cost = 0
            
            # Recent jobs
            try:
                cur.execute("SELECT * FROM scrape_jobs ORDER BY created_at DESC LIMIT 5")
                jobs = [dict_row(r) for r in cur.fetchall()]
            except:
                if USE_POSTGRES:
                    try:
                        conn.rollback()
                    except:
                        pass
                jobs = []
            
            cur.close()
            conn.close()
            
            for j in jobs:
                if j:
                    for k, v in j.items():
                        j[k] = dt_to_str(v)
            
            return jsonify({
                'restaurants': rest_count, 'reviews': rev_count, 'user_reviews': user_rev,
                'published': pub_count, 'pushed': pushed, 'regions': regions,
                'monthly_cost': round(cost or 0, 2), 'recent_jobs': jobs
            })
        except Exception as e:
            app.logger.error(f'Stats error: {str(e)}')
            return jsonify({'error': str(e), 'restaurants': 0, 'reviews': 0, 'user_reviews': 0,
                'published': 0, 'pushed': 0, 'regions': 0, 'monthly_cost': 0, 'recent_jobs': []})

    # ========== COMPREHENSIVE HEALTH CHECK ==========
    
    @app.route('/api/health/full')
    def full_health_check():
        """Comprehensive health check for the entire application"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'version': '4.2',
            'database': {'status': 'unknown', 'checks': []},
            'tables': {'status': 'unknown', 'checks': []},
            'data_integrity': {'status': 'unknown', 'checks': []},
            'api_endpoints': {'status': 'unknown', 'checks': []},
            'external_services': {'status': 'unknown', 'checks': []},
            'configuration': {'status': 'unknown', 'checks': []},
            'summary': {'total': 0, 'passed': 0, 'failed': 0, 'warnings': 0}
        }
        
        def add_check(category, name, passed, message, warning=False):
            status = 'pass' if passed else ('warning' if warning else 'fail')
            results[category]['checks'].append({
                'name': name,
                'status': status,
                'message': message
            })
            results['summary']['total'] += 1
            if passed:
                results['summary']['passed'] += 1
            elif warning:
                results['summary']['warnings'] += 1
            else:
                results['summary']['failed'] += 1
        
        # ========== DATABASE CHECKS ==========
        try:
            conn = get_db()
            cur = get_cursor(conn)
            add_check('database', 'Connection', True, f"Connected to {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
            
            # Check database type
            try:
                if USE_POSTGRES:
                    cur.execute("SELECT version() as v")
                    row = cur.fetchone()
                    version = row['v'] if hasattr(row, 'keys') else row[0]
                    add_check('database', 'Version', True, version[:50] + '...' if len(str(version)) > 50 else str(version))
                else:
                    cur.execute("SELECT sqlite_version()")
                    version = cur.fetchone()[0]
                    add_check('database', 'Version', True, f"SQLite {version}")
            except Exception as ve:
                add_check('database', 'Version', False, f"Version check failed: {str(ve)[:50]}")
            
        except Exception as e:
            add_check('database', 'Connection', False, str(e))
            results['database']['status'] = 'fail'
            return jsonify(results)
        
        # ========== TABLE CHECKS ==========
        required_tables = [
            'restaurants', 'reviews', 'rankings', 'regions', 'api_keys',
            'api_usage', 'scrape_jobs', 'ranking_weights', 'website_config',
            'email_config', 'email_queue', 'push_history', 'sentiment_keywords'
        ]
        
        for table in required_tables:
            try:
                cur.execute(f"SELECT COUNT(*) as c FROM {table}")
                row = cur.fetchone()
                count = row['c'] if hasattr(row, 'keys') else row[0]
                add_check('tables', f"Table: {table}", True, f"{count} rows")
            except Exception as e:
                add_check('tables', f"Table: {table}", False, str(e)[:100])
                # PostgreSQL requires rollback after error to continue
                if USE_POSTGRES:
                    try:
                        conn.rollback()
                    except:
                        pass
        
        # ========== DATA INTEGRITY CHECKS ==========
        def get_count(row):
            """Helper to get count value from cursor row"""
            if row is None:
                return 0
            if hasattr(row, 'keys'):
                # RealDictRow - try common column names
                for key in ['c', 'count', 'cnt']:
                    if key in row:
                        return row[key]
                # Return first value
                return list(row.values())[0] if row else 0
            return row[0]
        
        def safe_query(sql, default=0):
            """Execute query with rollback on error for PostgreSQL"""
            try:
                cur.execute(sql)
                return get_count(cur.fetchone())
            except Exception as e:
                if USE_POSTGRES:
                    try:
                        conn.rollback()
                    except:
                        pass
                return default
        
        try:
            # Check restaurants
            active_restaurants = safe_query("SELECT COUNT(*) as c FROM restaurants WHERE is_active=1")
            add_check('data_integrity', 'Active Restaurants', active_restaurants > 0, 
                     f"{active_restaurants} active restaurants", warning=(active_restaurants == 0))
            
            # Check for restaurants without rankings
            orphan_restaurants = safe_query("""SELECT COUNT(*) as c FROM restaurants r 
                          LEFT JOIN rankings rk ON r.id = rk.restaurant_id 
                          WHERE r.is_active=1 AND rk.id IS NULL""")
            add_check('data_integrity', 'Restaurants without Rankings', orphan_restaurants == 0,
                     f"{orphan_restaurants} restaurants missing rankings", warning=(orphan_restaurants > 0))
            
            # Check regions
            region_count = safe_query("SELECT COUNT(*) as c FROM regions")
            add_check('data_integrity', 'Regions Configured', region_count > 0,
                     f"{region_count} regions", warning=(region_count == 0))
            
            # Check for restaurants with website but no email
            needs_email = safe_query("SELECT COUNT(*) as c FROM restaurants WHERE website IS NOT NULL AND website != '' AND (email IS NULL OR email = '')")
            add_check('data_integrity', 'Restaurants Needing Email Extraction', True,
                     f"{needs_email} restaurants have website but no email", warning=(needs_email > 10))
            
            # Check reviews
            review_count = safe_query("SELECT COUNT(*) as c FROM reviews")
            add_check('data_integrity', 'Reviews', True, f"{review_count} total reviews")
            
            # Check reviews with sentiment analysis
            analyzed_reviews = safe_query("SELECT COUNT(*) as c FROM reviews WHERE text_polarity IS NOT NULL")
            pct = (analyzed_reviews / review_count * 100) if review_count > 0 else 0
            add_check('data_integrity', 'Reviews with NLP Sentiment', True,
                     f"{analyzed_reviews}/{review_count} ({pct:.1f}%) analyzed", warning=(pct < 50 and review_count > 0))
            
            # Check for duplicate restaurants
            try:
                cur.execute("""SELECT name, canton, COUNT(*) as cnt FROM restaurants 
                              WHERE is_active=1 GROUP BY name, canton HAVING COUNT(*) > 1""")
                duplicates = cur.fetchall()
                add_check('data_integrity', 'Duplicate Restaurants', len(duplicates) == 0,
                         f"{len(duplicates)} potential duplicates found", warning=(len(duplicates) > 0))
            except:
                if USE_POSTGRES:
                    conn.rollback()
                add_check('data_integrity', 'Duplicate Restaurants', True, 'Check skipped')
            
            # Check ranking weights
            weights = safe_query("SELECT COUNT(*) as c FROM ranking_weights WHERE is_active=1")
            add_check('data_integrity', 'Ranking Weights Configured', weights > 0,
                     f"{weights} active weight configurations", warning=(weights == 0))
            
            # Check sentiment keywords (table might not exist)
            keywords = safe_query("SELECT COUNT(*) as c FROM sentiment_keywords WHERE is_active=1")
            add_check('data_integrity', 'Custom Sentiment Keywords', True,
                     f"{keywords} custom keywords defined")
            
        except Exception as e:
            add_check('data_integrity', 'Data Check Error', False, str(e)[:100])
            if USE_POSTGRES:
                try:
                    conn.rollback()
                except:
                    pass
        
        # ========== CONFIGURATION CHECKS ==========
        try:
            # Check API keys
            cur.execute("SELECT provider, is_active FROM api_keys")
            api_keys = cur.fetchall()
            for key in api_keys:
                key_dict = dict_row(key) if hasattr(key, 'keys') else {'provider': key[0], 'is_active': key[1]}
                status = key_dict.get('is_active', 0)
                add_check('configuration', f"API Key: {key_dict['provider']}", status == 1,
                         'Configured and active' if status == 1 else 'Not active', warning=(status != 1))
            
            if not api_keys:
                add_check('configuration', 'API Keys', False, 'No API keys configured', warning=True)
            
            # Check website config
            cur.execute("SELECT * FROM website_config LIMIT 1")
            website_config = cur.fetchone()
            if website_config:
                wc = dict_row(website_config) if hasattr(website_config, 'keys') else {}
                has_url = bool(wc.get('api_url') or wc.get('base_url'))
                add_check('configuration', 'Website Push Config', has_url,
                         'Configured' if has_url else 'Not configured', warning=(not has_url))
            else:
                add_check('configuration', 'Website Push Config', False, 'Not configured', warning=True)
            
            # Check email config
            cur.execute("SELECT * FROM email_config LIMIT 1")
            email_config = cur.fetchone()
            if email_config:
                ec = dict_row(email_config) if hasattr(email_config, 'keys') else {}
                has_smtp = bool(ec.get('smtp_host'))
                add_check('configuration', 'Email SMTP Config', has_smtp,
                         'Configured' if has_smtp else 'Not configured', warning=(not has_smtp))
            else:
                add_check('configuration', 'Email SMTP Config', False, 'Not configured', warning=True)
            
        except Exception as e:
            add_check('configuration', 'Config Check Error', False, str(e)[:100])
            if USE_POSTGRES:
                try:
                    conn.rollback()
                except:
                    pass
        
        # ========== EXTERNAL SERVICES ==========
        try:
            # Check Google Places API
            cur.execute("SELECT api_key FROM api_keys WHERE provider='google_places' AND is_active=1")
            google_key = cur.fetchone()
            if google_key:
                key_val = google_key[0] if not hasattr(google_key, 'keys') else google_key['api_key']
                # Quick API test
                try:
                    import requests
                    test_url = f"https://places.googleapis.com/v1/places:searchText"
                    headers = {
                        'Content-Type': 'application/json',
                        'X-Goog-Api-Key': key_val,
                        'X-Goog-FieldMask': 'places.id'
                    }
                    resp = requests.post(test_url, json={'textQuery': 'test', 'maxResultCount': 1}, 
                                        headers=headers, timeout=5)
                    if resp.status_code == 200:
                        add_check('external_services', 'Google Places API', True, 'Connected and working')
                    elif resp.status_code == 403:
                        add_check('external_services', 'Google Places API', False, 'API key invalid or quota exceeded')
                    else:
                        add_check('external_services', 'Google Places API', False, f'Status {resp.status_code}')
                except requests.exceptions.Timeout:
                    add_check('external_services', 'Google Places API', False, 'Connection timeout', warning=True)
                except Exception as e:
                    add_check('external_services', 'Google Places API', False, str(e)[:50], warning=True)
            else:
                add_check('external_services', 'Google Places API', False, 'No API key configured', warning=True)
            
        except Exception as e:
            add_check('external_services', 'External Services Error', False, str(e)[:100])
            if USE_POSTGRES:
                try:
                    conn.rollback()
                except:
                    pass
        
        # ========== API ENDPOINT CHECKS ==========
        endpoints_to_check = [
            ('/api/stats', 'Stats API'),
            ('/api/regions', 'Regions API'),
            ('/api/restaurants', 'Restaurants API'),
            ('/api/rankings', 'Rankings API'),
            ('/api/keys', 'API Keys'),
            ('/api/costs', 'Cost Analytics'),
            ('/api/sentiment/stats', 'Sentiment Stats'),
            ('/api/sentiment/keywords', 'Sentiment Keywords'),
        ]
        
        for endpoint, name in endpoints_to_check:
            try:
                # Use test client
                with app.test_client() as client:
                    resp = client.get(endpoint)
                    if resp.status_code == 200:
                        data = resp.get_json()
                        if isinstance(data, dict) and 'error' in data:
                            add_check('api_endpoints', name, False, data['error'][:50])
                        else:
                            add_check('api_endpoints', name, True, f"OK (status {resp.status_code})")
                    else:
                        add_check('api_endpoints', name, False, f"Status {resp.status_code}")
            except Exception as e:
                add_check('api_endpoints', name, False, str(e)[:50])
        
        cur.close()
        conn.close()
        
        # Calculate category statuses
        for category in ['database', 'tables', 'data_integrity', 'api_endpoints', 'external_services', 'configuration']:
            checks = results[category]['checks']
            if not checks:
                results[category]['status'] = 'unknown'
            elif all(c['status'] == 'pass' for c in checks):
                results[category]['status'] = 'pass'
            elif any(c['status'] == 'fail' for c in checks):
                results[category]['status'] = 'fail'
            else:
                results[category]['status'] = 'warning'
        
        # Overall status
        if results['summary']['failed'] > 0:
            results['overall_status'] = 'fail'
        elif results['summary']['warnings'] > 0:
            results['overall_status'] = 'warning'
        else:
            results['overall_status'] = 'pass'
        
        return jsonify(results)

    # ========== REGIONS ==========
    
    @app.route('/api/regions')
    def get_regions():
        try:
            conn = get_db()
            cur = get_cursor(conn)
            cur.execute("SELECT * FROM regions ORDER BY name")
            rows = [dict_row(r) for r in cur.fetchall()]
            
            # Get restaurant and published counts per region using a separate query
            p = '%s' if USE_POSTGRES else '?'
            for r in rows:
                if r and r.get('code'):
                    try:
                        code = r['code']
                        name = r.get('name', code)
                        
                        # Count restaurants in this region - match by canton OR by city name
                        cur.execute(f"""SELECT COUNT(*) as cnt FROM restaurants 
                            WHERE is_active=1 AND (
                                canton={p} OR 
                                LOWER(canton)=LOWER({p}) OR 
                                LOWER(city)=LOWER({p}) OR
                                LOWER(city)=LOWER({p})
                            )""", 
                            (code, code, code, name))
                        count_row = cur.fetchone()
                        if count_row:
                            r['restaurants_count'] = count_row['cnt'] if isinstance(count_row, dict) else count_row[0]
                        else:
                            r['restaurants_count'] = 0
                        
                        # Count published
                        cur.execute(f"""SELECT COUNT(*) as cnt FROM rankings rk
                            JOIN restaurants rest ON rk.restaurant_id=rest.id
                            WHERE rk.is_published=1 AND rest.is_active=1 
                            AND (
                                rest.canton={p} OR 
                                LOWER(rest.canton)=LOWER({p}) OR 
                                LOWER(rest.city)=LOWER({p}) OR
                                LOWER(rest.city)=LOWER({p})
                            )""",
                            (code, code, code, name))
                        pub_row = cur.fetchone()
                        if pub_row:
                            r['published_count'] = pub_row['cnt'] if isinstance(pub_row, dict) else pub_row[0]
                        else:
                            r['published_count'] = 0
                            
                        app.logger.debug(f"Region {code}/{name}: {r['restaurants_count']} restaurants, {r['published_count']} published")
                        
                    except Exception as count_err:
                        app.logger.warning(f"Error counting for region {r.get('code')}: {count_err}")
                        r['restaurants_count'] = r.get('restaurants_count', 0)
                        r['published_count'] = 0
            
            cur.close()
            conn.close()
            
            now = datetime.now()
            for r in rows:
                if r:
                    last_scan = r.get('last_scan')
                    if last_scan:
                        if isinstance(last_scan, str):
                            try:
                                last_scan = datetime.fromisoformat(last_scan.replace('Z', '+00:00').replace('+00:00', ''))
                            except:
                                last_scan = None
                        if last_scan:
                            days_ago = (now - last_scan).days
                            r['freshness'] = 'fresh' if days_ago < 30 else 'stale' if days_ago < 60 else 'old'
                            r['days_since_scan'] = days_ago
                        else:
                            r['freshness'] = 'never'
                            r['days_since_scan'] = None
                    else:
                        r['freshness'] = 'never'
                        r['days_since_scan'] = None
                    
                    for k, v in r.items():
                        r[k] = dt_to_str(v)
            
            return jsonify(rows)
        except Exception as e:
            app.logger.error(f'Regions error: {str(e)}')
            import traceback
            app.logger.error(traceback.format_exc())
            return jsonify([])
    
    @app.route('/api/regions', methods=['POST'])
    def add_region():
        data = request.json
        code = data.get('code', '').lower().replace(' ', '_').replace('-', '_')
        name = data.get('name', code.replace('_', ' ').title())
        
        conn = get_db()
        cur = conn.cursor()
        
        if USE_POSTGRES:
            cur.execute("""INSERT INTO regions (code, name, canton, latitude, longitude, zoom_level, auto_push, push_top_n)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                ON CONFLICT(code) DO UPDATE SET
                name=EXCLUDED.name, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                auto_push=EXCLUDED.auto_push, push_top_n=EXCLUDED.push_top_n""",
                (code, name, data.get('canton'), data.get('latitude'), data.get('longitude'),
                 data.get('zoom_level', 10), data.get('auto_push', 0), data.get('push_top_n', 20)))
        else:
            cur.execute("""INSERT INTO regions (code, name, canton, latitude, longitude, zoom_level, auto_push, push_top_n)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
                ON CONFLICT(code) DO UPDATE SET
                name=excluded.name, latitude=excluded.latitude, longitude=excluded.longitude,
                auto_push=excluded.auto_push, push_top_n=excluded.push_top_n""",
                (code, name, data.get('canton'), data.get('latitude'), data.get('longitude'),
                 data.get('zoom_level', 10), data.get('auto_push', 0), data.get('push_top_n', 20)))
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'code': code})
    
    @app.route('/api/regions/<code>', methods=['PUT'])
    def update_region(code):
        data = request.json
        conn = get_db()
        cur = conn.cursor()
        
        if USE_POSTGRES:
            cur.execute("""UPDATE regions SET 
                auto_push=COALESCE(%s, auto_push), push_top_n=COALESCE(%s, push_top_n)
                WHERE code=%s""",
                (data.get('auto_push'), data.get('push_top_n'), code))
        else:
            cur.execute("""UPDATE regions SET 
                auto_push=COALESCE(?, auto_push), push_top_n=COALESCE(?, push_top_n)
                WHERE code=?""",
                (data.get('auto_push'), data.get('push_top_n'), code))
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})

    @app.route('/api/regions/merge-duplicates', methods=['POST'])
    def merge_region_duplicates():
        """Merge duplicate region entries based on name"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Find duplicates by name (case-insensitive)
            if USE_POSTGRES:
                cur.execute("""
                    SELECT LOWER(name) as lname, COUNT(*) as cnt, array_agg(code) as codes
                    FROM regions 
                    WHERE name IS NOT NULL
                    GROUP BY LOWER(name)
                    HAVING COUNT(*) > 1
                """)
            else:
                cur.execute("""
                    SELECT LOWER(name) as lname, COUNT(*) as cnt, GROUP_CONCAT(code) as codes
                    FROM regions 
                    WHERE name IS NOT NULL
                    GROUP BY LOWER(name)
                    HAVING COUNT(*) > 1
                """)
            
            duplicates = cur.fetchall()
            merged = 0
            
            for dup in duplicates:
                dup_dict = dict_row(dup)
                codes_str = dup_dict.get('codes', '')
                
                if USE_POSTGRES:
                    codes = codes_str if isinstance(codes_str, list) else [codes_str]
                else:
                    codes = codes_str.split(',') if codes_str else []
                
                if len(codes) < 2:
                    continue
                
                # Keep the first code, merge others into it
                keep_code = codes[0]
                remove_codes = codes[1:]
                
                # Update restaurants to use the kept region code
                for old_code in remove_codes:
                    if USE_POSTGRES:
                        cur.execute("UPDATE restaurants SET canton=%s WHERE canton=%s", (keep_code, old_code))
                    else:
                        cur.execute("UPDATE restaurants SET canton=? WHERE canton=?", (keep_code, old_code))
                
                # Delete duplicate regions
                for old_code in remove_codes:
                    if USE_POSTGRES:
                        cur.execute("DELETE FROM regions WHERE code=%s", (old_code,))
                    else:
                        cur.execute("DELETE FROM regions WHERE code=?", (old_code,))
                    merged += 1
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'merged': merged})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    # ========== API KEYS ==========
    
    @app.route('/api/keys', methods=['GET'])
    def get_keys():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT provider, api_key_encrypted, is_active, created_at, updated_at FROM api_keys")
        rows = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        
        keys = []
        for r in rows:
            if r:
                r['is_configured'] = True
                r['is_active'] = bool(r.get('is_active'))
                # Extract preview from stored format: KEY:...|PREVIEW:...
                encrypted = r.get('api_key_encrypted', '')
                if encrypted and '|PREVIEW:' in encrypted:
                    r['key_preview'] = encrypted.split('|PREVIEW:')[1]
                elif encrypted:
                    # Old format - just show what we have
                    r['key_preview'] = encrypted[:12] + '...' if len(encrypted) > 12 else encrypted
                else:
                    r['key_preview'] = None
                del r['api_key_encrypted']  # Don't send encoded key
                for k, v in r.items():
                    r[k] = dt_to_str(v)
                keys.append(r)
        
        configured = {k['provider'] for k in keys}
        for p in ['google', 'yelp', 'tripadvisor']:
            if p not in configured:
                keys.append({'provider': p, 'is_configured': False, 'is_active': False, 'key_preview': None})
        
        return jsonify(keys)
    
    def get_api_key_from_db(provider):
        """Get actual API key from database (stored as base64)"""
        import base64
        
        # First try config file
        cfg = load_config()
        if cfg.get('api_keys', {}).get(provider):
            return cfg['api_keys'][provider]
        
        # Then try database
        conn = get_db()
        cur = get_cursor(conn)
        if USE_POSTGRES:
            cur.execute("SELECT api_key_encrypted FROM api_keys WHERE provider=%s AND is_active=1", (provider,))
        else:
            cur.execute("SELECT api_key_encrypted FROM api_keys WHERE provider=? AND is_active=1", (provider,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            encrypted = dict_row(row).get('api_key_encrypted', '') if hasattr(row, 'keys') else (row[0] if row else '')
            if encrypted and encrypted.startswith('KEY:'):
                # Extract base64 encoded key
                try:
                    encoded = encrypted.split('|')[0].replace('KEY:', '')
                    return base64.b64decode(encoded).decode()
                except:
                    pass
        return None
    
    @app.route('/api/keys/<provider>/health', methods=['GET'])
    def check_key_health(provider):
        """Test if an API key is valid and working"""
        import requests as req
        
        # Get the actual API key
        api_key = get_api_key_from_db(provider)
        
        if not api_key:
            return jsonify({'status': 'not_configured', 'message': 'API key not found'})
        
        try:
            if provider == 'google':
                # Test Google Places API (New) with a simple request
                url = "https://places.googleapis.com/v1/places:searchNearby"
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': api_key,
                    'X-Goog-FieldMask': 'places.displayName'
                }
                body = {
                    'includedTypes': ['restaurant'],
                    'maxResultCount': 1,
                    'locationRestriction': {
                        'circle': {
                            'center': {'latitude': 46.2044, 'longitude': 6.1432},
                            'radius': 100.0
                        }
                    }
                }
                response = req.post(url, headers=headers, json=body, timeout=10)
                
                if response.status_code == 200:
                    return jsonify({'status': 'healthy', 'message': 'Google Places API (New) is working'})
                elif response.status_code == 403:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error', {}).get('message', 'Access denied')
                    return jsonify({'status': 'error', 'message': f"API access denied: {error_msg}"})
                elif response.status_code == 400:
                    error_data = response.json() if response.text else {}
                    error_msg = error_data.get('error', {}).get('message', 'Bad request')
                    return jsonify({'status': 'error', 'message': f"Bad request: {error_msg}"})
                else:
                    return jsonify({'status': 'warning', 'message': f"API returned status {response.status_code}"})
                    
            elif provider == 'yelp':
                # Test Yelp API
                url = "https://api.yelp.com/v3/businesses/search?location=Geneva&limit=1"
                headers = {'Authorization': f'Bearer {api_key}'}
                response = req.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    return jsonify({'status': 'healthy', 'message': 'Yelp API is working'})
                elif response.status_code == 401:
                    return jsonify({'status': 'error', 'message': 'Invalid API key or unauthorized'})
                else:
                    return jsonify({'status': 'error', 'message': f'API returned status {response.status_code}'})
                    
            elif provider == 'tripadvisor':
                # TripAdvisor API test
                url = f"https://api.content.tripadvisor.com/api/v1/location/search?key={api_key}&searchQuery=restaurant&language=en"
                response = req.get(url, timeout=10)
                
                if response.status_code == 200:
                    return jsonify({'status': 'healthy', 'message': 'TripAdvisor API is working'})
                elif response.status_code == 401 or response.status_code == 403:
                    return jsonify({'status': 'error', 'message': 'Invalid API key or unauthorized'})
                else:
                    return jsonify({'status': 'warning', 'message': f'API returned status {response.status_code}'})
            else:
                return jsonify({'status': 'unknown', 'message': f'Unknown provider: {provider}'})
                
        except req.exceptions.Timeout:
            return jsonify({'status': 'error', 'message': 'API request timed out'})
        except req.exceptions.ConnectionError:
            return jsonify({'status': 'error', 'message': 'Could not connect to API'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})
    
    @app.route('/api/keys', methods=['POST'])
    def save_key():
        data = request.json
        provider = data.get('provider')
        api_key = data.get('api_key')
        if not provider or not api_key:
            return jsonify({'error': 'Missing data'}), 400
        
        # Create a readable preview: first 8 chars ... last 4 chars
        if len(api_key) > 12:
            key_preview = api_key[:8] + '...' + api_key[-4:]
        else:
            key_preview = '••••' + api_key[-4:] if len(api_key) > 4 else '••••••••'
        
        # Store the ACTUAL key (base64 encoded for basic obfuscation - NOT secure encryption)
        import base64
        encoded_key = base64.b64encode(api_key.encode()).decode()
        
        conn = get_db()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("""INSERT INTO api_keys (provider, api_key_encrypted, is_active)
                VALUES (%s, %s, 1) ON CONFLICT(provider) DO UPDATE SET
                api_key_encrypted=EXCLUDED.api_key_encrypted, is_active=1, updated_at=NOW()""",
                (provider, f"KEY:{encoded_key}|PREVIEW:{key_preview}"))
        else:
            cur.execute("""INSERT INTO api_keys (provider, api_key_encrypted, is_active)
                VALUES (?, ?, 1) ON CONFLICT(provider) DO UPDATE SET
                api_key_encrypted=excluded.api_key_encrypted, is_active=1, updated_at=CURRENT_TIMESTAMP""",
                (provider, f"KEY:{encoded_key}|PREVIEW:{key_preview}"))
        conn.commit()
        cur.close()
        conn.close()
        
        # Also save to config file as backup
        cfg = load_config()
        cfg['api_keys'][provider] = api_key
        save_config(cfg)
        
        return jsonify({'success': True, 'preview': key_preview})
    
    @app.route('/api/keys/<provider>/toggle', methods=['POST'])
    def toggle_key(provider):
        conn = get_db()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("UPDATE api_keys SET is_active=CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE provider=%s", (provider,))
        else:
            cur.execute("UPDATE api_keys SET is_active=NOT is_active WHERE provider=?", (provider,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})

    @app.route('/api/keys/<provider>', methods=['DELETE'])
    def delete_key(provider):
        """Delete an API key"""
        conn = get_db()
        cur = conn.cursor()
        try:
            if USE_POSTGRES:
                cur.execute("DELETE FROM api_keys WHERE provider=%s", (provider,))
            else:
                cur.execute("DELETE FROM api_keys WHERE provider=?", (provider,))
            conn.commit()
            deleted = cur.rowcount > 0
            cur.close()
            conn.close()
            return jsonify({'success': True, 'deleted': deleted})
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': str(e)}), 500

    # ========== COST ANALYTICS ==========
    
    @app.route('/api/costs', methods=['GET'])
    def get_costs():
        """Get API cost analytics"""
        period = request.args.get('period', 'week')
        
        conn = get_db()
        cur = get_cursor(conn)
        
        def get_value(row, default=0):
            """Get value from cursor row (handles both dict and tuple)"""
            if row is None:
                return default
            if hasattr(row, 'keys'):
                # RealDictRow - get first value
                return list(row.values())[0] if row else default
            return row[0] if row else default
        
        try:
            # Determine date filter
            if USE_POSTGRES:
                if period == 'today':
                    date_filter = "timestamp >= CURRENT_DATE"
                elif period == 'week':
                    date_filter = "timestamp >= CURRENT_DATE - INTERVAL '7 days'"
                elif period == 'month':
                    date_filter = "timestamp >= CURRENT_DATE - INTERVAL '30 days'"
                else:
                    date_filter = "1=1"
            else:
                if period == 'today':
                    date_filter = "timestamp >= date('now')"
                elif period == 'week':
                    date_filter = "timestamp >= date('now', '-7 days')"
                elif period == 'month':
                    date_filter = "timestamp >= date('now', '-30 days')"
                else:
                    date_filter = "1=1"
            
            # Today's cost
            if USE_POSTGRES:
                cur.execute("SELECT COALESCE(SUM(estimated_cost), 0) as c FROM api_usage WHERE timestamp >= CURRENT_DATE")
            else:
                cur.execute("SELECT COALESCE(SUM(estimated_cost), 0) as c FROM api_usage WHERE timestamp >= date('now')")
            today_cost = get_value(cur.fetchone(), 0)
            
            # This month's cost
            if USE_POSTGRES:
                cur.execute("SELECT COALESCE(SUM(estimated_cost), 0) as c FROM api_usage WHERE timestamp >= date_trunc('month', CURRENT_DATE)")
            else:
                cur.execute("SELECT COALESCE(SUM(estimated_cost), 0) as c FROM api_usage WHERE timestamp >= date('now', 'start of month')")
            month_cost = get_value(cur.fetchone(), 0)
            
            # Total calls and cost
            cur.execute("SELECT COALESCE(SUM(request_count), 0) as calls, COALESCE(SUM(estimated_cost), 0) as cost FROM api_usage")
            row = cur.fetchone()
            if hasattr(row, 'keys'):
                total_calls = row.get('calls', 0) or 0
                total_cost = row.get('cost', 0) or 0
            else:
                total_calls = row[0] if row else 0
                total_cost = row[1] if row else 0
            
            # Breakdown by endpoint
            cur.execute(f"""
                SELECT provider, endpoint, 
                    SUM(request_count) as request_count,
                    AVG(estimated_cost / NULLIF(request_count, 0)) as cost_per_call,
                    SUM(estimated_cost) as total_cost
                FROM api_usage 
                WHERE {date_filter}
                GROUP BY provider, endpoint
                ORDER BY total_cost DESC
            """)
            breakdown = []
            for row in cur.fetchall():
                r = dict_row(row)
                if r:
                    breakdown.append({
                        'provider': r.get('provider', 'unknown'),
                        'endpoint': r.get('endpoint', 'unknown'),
                        'request_count': int(r.get('request_count', 0)),
                        'cost_per_call': float(r.get('cost_per_call', 0) or 0),
                        'total_cost': float(r.get('total_cost', 0))
                    })
            
            # Recent history
            cur.execute(f"""
                SELECT provider, endpoint, request_count, estimated_cost, timestamp
                FROM api_usage 
                WHERE {date_filter}
                ORDER BY timestamp DESC
                LIMIT 100
            """)
            history = []
            for row in cur.fetchall():
                r = dict_row(row)
                if r:
                    history.append({
                        'provider': r.get('provider', 'unknown'),
                        'endpoint': r.get('endpoint', 'unknown'),
                        'request_count': int(r.get('request_count', 0)),
                        'estimated_cost': float(r.get('estimated_cost', 0)),
                        'timestamp': dt_to_str(r.get('timestamp'))
                    })
            
            cur.close()
            conn.close()
            
            return jsonify({
                'today_cost': float(today_cost),
                'month_cost': float(month_cost),
                'total_calls': int(total_calls),
                'total_cost': float(total_cost),
                'breakdown': breakdown,
                'history': history
            })
            
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/costs/track', methods=['POST'])
    def track_cost():
        """Track API usage"""
        data = request.json
        provider = data.get('provider', 'unknown')
        endpoint = data.get('endpoint', 'unknown')
        request_count = data.get('request_count', 1)
        estimated_cost = data.get('estimated_cost', 0)
        
        conn = get_db()
        cur = conn.cursor()
        
        try:
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (provider, endpoint, request_count, estimated_cost))
            else:
                cur.execute("""
                    INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                    VALUES (?, ?, ?, ?, datetime('now'))
                """, (provider, endpoint, request_count, estimated_cost))
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
            
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/costs/clear', methods=['POST'])
    def clear_costs():
        """Clear API usage history"""
        conn = get_db()
        cur = conn.cursor()
        
        try:
            cur.execute("DELETE FROM api_usage")
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'error': str(e)}), 500

    # ========== CUSTOM PROVIDERS ==========
    
    @app.route('/api/providers/custom', methods=['GET'])
    def get_custom_providers():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT * FROM custom_providers ORDER BY name")
        rows = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r:
                for k, v in r.items():
                    r[k] = dt_to_str(v)
        return jsonify(rows)
    
    @app.route('/api/providers/custom', methods=['POST'])
    def add_custom_provider():
        data = request.json
        name = data.get('name', '').lower().replace(' ', '_')
        if not name:
            return jsonify({'error': 'Name required'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        if USE_POSTGRES:
            cur.execute("""INSERT INTO custom_providers (name, display_name, api_endpoint, auth_type, rate_limit, cost_per_request)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(name) DO UPDATE SET
                display_name=EXCLUDED.display_name, api_endpoint=EXCLUDED.api_endpoint""",
                (name, data.get('display_name', name), data.get('api_endpoint'),
                 data.get('auth_type', 'api_key'), data.get('rate_limit', 100), data.get('cost_per_request', 0)))
        else:
            cur.execute("""INSERT INTO custom_providers (name, display_name, api_endpoint, auth_type, rate_limit, cost_per_request)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET
                display_name=excluded.display_name, api_endpoint=excluded.api_endpoint""",
                (name, data.get('display_name', name), data.get('api_endpoint'),
                 data.get('auth_type', 'api_key'), data.get('rate_limit', 100), data.get('cost_per_request', 0)))
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    
    @app.route('/api/providers/custom/<name>', methods=['DELETE'])
    def delete_custom_provider(name):
        conn = get_db()
        cur = conn.cursor()
        p = '%s' if USE_POSTGRES else '?'
        cur.execute(f"DELETE FROM custom_providers WHERE name={p}", (name,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})

    # ========== RANKING WEIGHTS ==========
    
    @app.route('/api/ranking/weights', methods=['GET'])
    def get_ranking_weights():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT * FROM ranking_weights WHERE is_active=1 LIMIT 1")
        row = cur.fetchone()
        
        cur.execute("SELECT name FROM custom_providers WHERE is_active=1")
        custom = [r['name'] if USE_POSTGRES else r[0] for r in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        if row:
            w = dict_row(row)
            if w.get('custom_weights'):
                try:
                    w['custom_provider_weights'] = json.loads(w['custom_weights'])
                except:
                    w['custom_provider_weights'] = {}
            else:
                w['custom_provider_weights'] = {}
            w['available_custom_providers'] = custom
            return jsonify(w)
        
        return jsonify({
            'weight_google': 0.25, 'weight_yelp': 0.20, 'weight_tripadvisor': 0.25,
            'weight_user_reviews': 0.15, 'weight_sentiment': 0.10, 'weight_text_sentiment': 0.50,
            'weight_data_quality': 0.05,
            'min_reviews_threshold': 5, 'recency_decay_days': 180,
            'custom_provider_weights': {}, 'available_custom_providers': custom
        })
    
    @app.route('/api/ranking/weights', methods=['POST'])
    def save_ranking_weights():
        data = request.json
        custom_weights = json.dumps(data.get('custom_provider_weights', {}))
        
        conn = get_db()
        cur = conn.cursor()
        
        if USE_POSTGRES:
            cur.execute("""UPDATE ranking_weights SET
                weight_google=%s, weight_yelp=%s, weight_tripadvisor=%s,
                weight_user_reviews=%s, weight_sentiment=%s, weight_text_sentiment=%s, weight_data_quality=%s,
                min_reviews_threshold=%s, recency_decay_days=%s, custom_weights=%s,
                updated_at=NOW() WHERE is_active=1""",
                (data.get('weight_google', 0.25), data.get('weight_yelp', 0.20),
                 data.get('weight_tripadvisor', 0.25), data.get('weight_user_reviews', 0.15),
                 data.get('weight_sentiment', 0.10), data.get('weight_text_sentiment', 0.50),
                 data.get('weight_data_quality', 0.05),
                 data.get('min_reviews_threshold', 5), data.get('recency_decay_days', 180),
                 custom_weights))
        else:
            cur.execute("""UPDATE ranking_weights SET
                weight_google=?, weight_yelp=?, weight_tripadvisor=?,
                weight_user_reviews=?, weight_sentiment=?, weight_text_sentiment=?, weight_data_quality=?,
                min_reviews_threshold=?, recency_decay_days=?, custom_weights=?,
                updated_at=CURRENT_TIMESTAMP WHERE is_active=1""",
                (data.get('weight_google', 0.25), data.get('weight_yelp', 0.20),
                 data.get('weight_tripadvisor', 0.25), data.get('weight_user_reviews', 0.15),
                 data.get('weight_sentiment', 0.10), data.get('weight_text_sentiment', 0.50),
                 data.get('weight_data_quality', 0.05),
                 data.get('min_reviews_threshold', 5), data.get('recency_decay_days', 180),
                 custom_weights))
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})

    # ========== DATA COLLECTION ==========
    
    @app.route('/api/scrape/start', methods=['POST'])
    def start_scrape():
        """Legacy endpoint - redirects to new search workflow"""
        return jsonify({'error': 'Please use the new Search & Import workflow'}), 400
    
    # ========== NEW DATA COLLECTION WORKFLOW ==========
    
    @app.route('/api/places/search', methods=['POST'])
    def search_places():
        """Search for restaurants in a location using Google Places API (New) or mock data"""
        try:
            data = request.json
            city = data.get('city', '')
            country = data.get('country', '')
            radius_km = data.get('radius', 5)  # Default 5km
            min_rating = data.get('min_rating', 0)
            min_reviews = data.get('min_reviews', 0)
            max_results = data.get('max_results', 60)  # Default 60, user can request more
            
            if not city:
                return jsonify({'error': 'City is required'}), 400
            
            # Get API key for Google Places using our helper function
            google_api_key = get_api_key_from_db('google')
            
            results = []
            api_error = None
            seen_place_ids = set()  # Avoid duplicates from multiple API calls
            
            if google_api_key:
                # Use real Google Places API (New)
                import requests
                
                try:
                    # First, geocode the city using Geocoding API
                    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city},{country}&key={google_api_key}"
                    geo_res = requests.get(geocode_url, timeout=10)
                    geo_data = geo_res.json()
                    
                    if geo_data.get('status') == 'REQUEST_DENIED':
                        api_error = f"Geocoding API denied: {geo_data.get('error_message', 'Enable Geocoding API')}"
                    elif geo_data.get('results'):
                        location = geo_data['results'][0]['geometry']['location']
                        center_lat, center_lng = location['lat'], location['lng']
                        
                        # Calculate search points for broader coverage
                        # For larger searches, use multiple center points in a grid
                        search_points = [(center_lat, center_lng)]  # Always include center
                        
                        if max_results > 20 and radius_km >= 3:
                            # Add offset points for more coverage (N, S, E, W)
                            offset = radius_km * 0.5 / 111  # Rough km to degrees
                            search_points.extend([
                                (center_lat + offset, center_lng),  # North
                                (center_lat - offset, center_lng),  # South
                                (center_lat, center_lng + offset),  # East
                                (center_lat, center_lng - offset),  # West
                            ])
                        
                        places_url = "https://places.googleapis.com/v1/places:searchNearby"
                        headers = {
                            'Content-Type': 'application/json',
                            'X-Goog-Api-Key': google_api_key,
                            'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.priceLevel,places.location,places.types,places.primaryType,places.currentOpeningHours'
                        }
                        
                        # Search from each point until we have enough results
                        for lat, lng in search_points:
                            if len(results) >= max_results:
                                break
                                
                            body = {
                                'includedTypes': ['restaurant'],
                                'maxResultCount': 20,  # API max per request
                                'locationRestriction': {
                                    'circle': {
                                        'center': {'latitude': lat, 'longitude': lng},
                                        'radius': min(radius_km * 1000, 50000)  # Max 50km
                                    }
                                }
                            }
                            
                            places_res = requests.post(places_url, headers=headers, json=body, timeout=15)
                            
                            if places_res.status_code == 200:
                                places_data = places_res.json()
                                
                                for place in places_data.get('places', []):
                                    place_id = place.get('id', '')
                                    
                                    # Skip if already seen
                                    if place_id in seen_place_ids:
                                        continue
                                    seen_place_ids.add(place_id)
                                    
                                    rating = place.get('rating', 0)
                                    reviews = place.get('userRatingCount', 0)
                                    
                                    # Apply filters
                                    if rating >= min_rating and reviews >= min_reviews:
                                        # Map price level from new format
                                        price_map = {
                                            'PRICE_LEVEL_FREE': 0,
                                            'PRICE_LEVEL_INEXPENSIVE': 1,
                                            'PRICE_LEVEL_MODERATE': 2,
                                            'PRICE_LEVEL_EXPENSIVE': 3,
                                            'PRICE_LEVEL_VERY_EXPENSIVE': 4
                                        }
                                        price_level = price_map.get(place.get('priceLevel', ''), 2)
                                        
                                        results.append({
                                            'place_id': place_id,
                                            'name': place.get('displayName', {}).get('text', 'Unknown'),
                                            'address': place.get('formattedAddress', ''),
                                            'rating': rating,
                                            'review_count': reviews,
                                            'price_level': price_level,
                                            'latitude': place.get('location', {}).get('latitude', lat),
                                            'longitude': place.get('location', {}).get('longitude', lng),
                                            'types': place.get('types', []),
                                            'primary_type': place.get('primaryType', ''),
                                            'is_open': place.get('currentOpeningHours', {}).get('openNow', None)
                                        })
                                        
                                        if len(results) >= max_results:
                                            break
                            elif places_res.status_code == 403:
                                error_data = places_res.json() if places_res.text else {}
                                api_error = f"Places API denied: {error_data.get('error', {}).get('message', 'Enable Places API (New)')}"
                                break
                            else:
                                api_error = f"Places API returned status {places_res.status_code}"
                                break
                    else:
                        api_error = f"Could not geocode '{city}'"
                        
                except requests.exceptions.Timeout:
                    api_error = "API request timed out"
                except Exception as e:
                    api_error = str(e)
                    app.logger.error(f"Google API error: {e}")
            
            # If no API key or no results, generate mock data
            if not results:
                results = generate_mock_search_results(city, country, radius_km, min_rating, min_reviews)
            
            # Check for duplicates in existing database
            conn = get_db()
            cur = get_cursor(conn)
            for r in results:
                try:
                    if USE_POSTGRES:
                        cur.execute("SELECT id, name, city FROM restaurants WHERE LOWER(name)=LOWER(%s) AND LOWER(city)=LOWER(%s) LIMIT 1",
                            (r['name'], city))
                    else:
                        cur.execute("SELECT id, name, city FROM restaurants WHERE LOWER(name)=LOWER(?) AND LOWER(city)=LOWER(?) LIMIT 1",
                            (r['name'], city))
                    
                    existing = cur.fetchone()
                    if existing:
                        r['is_duplicate'] = True
                        r['existing_id'] = existing['id'] if isinstance(existing, dict) else existing[0]
                    else:
                        r['is_duplicate'] = False
                except:
                    r['is_duplicate'] = False
            
            cur.close()
            conn.close()
            
            response_data = {
                'success': True,
                'city': city,
                'country': country,
                'total_found': len(results),
                'duplicates': sum(1 for r in results if r.get('is_duplicate')),
                'new_restaurants': sum(1 for r in results if not r.get('is_duplicate')),
                'results': results,
                'source': 'google_api' if google_api_key and not api_error else 'mock_data'
            }
            
            if api_error:
                response_data['api_error'] = api_error
                response_data['note'] = 'Using mock data due to API error. Check API Keys section.'
            
            return jsonify(response_data)
            
        except Exception as e:
            import traceback
            app.logger.error(f"Search error: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/places/preview', methods=['POST'])
    def preview_places():
        """Quick preview of restaurants - FREE (Basic fields only)"""
        try:
            data = request.json
            city = data.get('city', '')
            country = data.get('country', '')
            radius_km = data.get('radius', 10)  # Default to 10km
            max_results = data.get('max_results', 200)  # Default to 200
            
            if not city:
                return jsonify({'error': 'City is required'}), 400
            
            # Detect large cities and increase search coverage
            large_cities = ['london', 'paris', 'new york', 'los angeles', 'chicago', 'tokyo', 'berlin', 'madrid', 'rome', 'barcelona', 'amsterdam', 'milan', 'munich', 'vienna', 'prague', 'brussels', 'dubai', 'singapore', 'hong kong', 'sydney', 'melbourne']
            is_large_city = city.lower() in large_cities or radius_km >= 15
            
            if is_large_city and radius_km < 15:
                radius_km = 20  # Use larger radius for big cities
            if is_large_city and max_results < 300:
                max_results = 500  # Get more results for big cities
            
            google_api_key = get_api_key_from_db('google')
            results = []
            api_error = None
            seen_place_ids = set()
            
            if google_api_key:
                import requests
                
                try:
                    # Geocode the city
                    geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city},{country}&key={google_api_key}"
                    geo_res = requests.get(geocode_url, timeout=10)
                    geo_data = geo_res.json()
                    
                    if geo_data.get('status') == 'REQUEST_DENIED':
                        api_error = f"Geocoding API denied: {geo_data.get('error_message', 'Enable Geocoding API')}"
                    elif geo_data.get('results'):
                        location = geo_data['results'][0]['geometry']['location']
                        center_lat, center_lng = location['lat'], location['lng']
                        
                        # Create grid of search points for more coverage
                        # For large cities, use many more points
                        search_points = [(center_lat, center_lng)]
                        
                        offset = radius_km * 0.4 / 111  # degrees offset (slightly larger)
                        
                        # Always add 4 cardinal directions
                        search_points.extend([
                            (center_lat + offset, center_lng),
                            (center_lat - offset, center_lng),
                            (center_lat, center_lng + offset),
                            (center_lat, center_lng - offset),
                        ])
                        
                        # Always add 4 diagonal directions
                        search_points.extend([
                            (center_lat + offset, center_lng + offset),
                            (center_lat + offset, center_lng - offset),
                            (center_lat - offset, center_lng + offset),
                            (center_lat - offset, center_lng - offset),
                        ])
                        
                        if max_results > 100 or is_large_city:
                            # Extended grid for better coverage
                            offset2 = offset * 1.8
                            search_points.extend([
                                (center_lat + offset2, center_lng),
                                (center_lat - offset2, center_lng),
                                (center_lat, center_lng + offset2),
                                (center_lat, center_lng - offset2),
                                (center_lat + offset2, center_lng + offset),
                                (center_lat + offset2, center_lng - offset),
                                (center_lat - offset2, center_lng + offset),
                                (center_lat - offset2, center_lng - offset),
                                (center_lat + offset, center_lng + offset2),
                                (center_lat + offset, center_lng - offset2),
                                (center_lat - offset, center_lng + offset2),
                                (center_lat - offset, center_lng - offset2),
                            ])
                        
                        if max_results > 300 or is_large_city:
                            # Full grid coverage for large cities
                            offset3 = offset * 2.5
                            for lat_mult in [-2, -1, 0, 1, 2]:
                                for lng_mult in [-2, -1, 0, 1, 2]:
                                    if abs(lat_mult) == 2 or abs(lng_mult) == 2:
                                        search_points.append((
                                            center_lat + offset * lat_mult,
                                            center_lng + offset * lng_mult
                                        ))
                        
                        app.logger.info(f"Preview search for {city}: {len(search_points)} search points, radius {radius_km}km, max {max_results}")
                        
                        places_url = "https://places.googleapis.com/v1/places:searchNearby"
                        # Basic fields only - FREE tier
                        headers = {
                            'Content-Type': 'application/json',
                            'X-Goog-Api-Key': google_api_key,
                            'X-Goog-FieldMask': 'places.id,places.displayName,places.types,places.primaryType,places.location,places.formattedAddress'
                        }
                        
                        for lat, lng in search_points:
                            if len(results) >= max_results:
                                break
                            
                            body = {
                                'includedTypes': ['restaurant'],
                                'maxResultCount': 20,
                                'locationRestriction': {
                                    'circle': {
                                        'center': {'latitude': lat, 'longitude': lng},
                                        'radius': min(radius_km * 1000, 50000)
                                    }
                                }
                            }
                            
                            places_res = requests.post(places_url, headers=headers, json=body, timeout=15)
                            
                            if places_res.status_code == 200:
                                places_data = places_res.json()
                                
                                for place in places_data.get('places', []):
                                    place_id = place.get('id', '')
                                    if place_id in seen_place_ids:
                                        continue
                                    seen_place_ids.add(place_id)
                                    
                                    results.append({
                                        'place_id': place_id,
                                        'name': place.get('displayName', {}).get('text', 'Unknown'),
                                        'address': place.get('formattedAddress', ''),
                                        'types': place.get('types', []),
                                        'primary_type': place.get('primaryType', 'restaurant'),
                                        'latitude': place.get('location', {}).get('latitude', lat),
                                        'longitude': place.get('location', {}).get('longitude', lng),
                                    })
                                    
                                    if len(results) >= max_results:
                                        break
                            elif places_res.status_code == 403:
                                error_data = places_res.json() if places_res.text else {}
                                api_error = f"Places API denied: {error_data.get('error', {}).get('message', 'Enable Places API (New)')}"
                                break
                    else:
                        api_error = f"Could not geocode '{city}'"
                        
                except requests.exceptions.Timeout:
                    api_error = "API request timed out"
                except Exception as e:
                    api_error = str(e)
            
            # Check for duplicates
            conn = get_db()
            cur = get_cursor(conn)
            for r in results:
                try:
                    if USE_POSTGRES:
                        cur.execute("SELECT id FROM restaurants WHERE LOWER(name)=LOWER(%s) AND LOWER(city)=LOWER(%s) LIMIT 1",
                            (r['name'], city))
                    else:
                        cur.execute("SELECT id FROM restaurants WHERE LOWER(name)=LOWER(?) AND LOWER(city)=LOWER(?) LIMIT 1",
                            (r['name'], city))
                    r['is_duplicate'] = cur.fetchone() is not None
                except:
                    r['is_duplicate'] = False
            cur.close()
            conn.close()
            
            response = {
                'success': True,
                'city': city,
                'country': country,
                'total_found': len(results),
                'results': results,
                'source': 'google_api' if google_api_key and not api_error else 'mock_data'
            }
            if api_error:
                response['api_error'] = api_error
            
            # Track API usage (FREE for basic fields)
            if google_api_key and not api_error and len(results) > 0:
                try:
                    conn2 = get_db()
                    cur2 = conn2.cursor()
                    if USE_POSTGRES:
                        cur2.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                            VALUES (%s, %s, %s, %s, NOW())""",
                            ('google', 'Quick Preview (Basic)', len(results), 0))  # FREE
                    else:
                        cur2.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                            VALUES (?, ?, ?, ?, datetime('now'))""",
                            ('google', 'Quick Preview (Basic)', len(results), 0))
                    conn2.commit()
                    cur2.close()
                    conn2.close()
                except Exception as track_err:
                    app.logger.warning(f"Failed to track API usage: {track_err}")
            
            return jsonify(response)
            
        except Exception as e:
            import traceback
            app.logger.error(f"Preview error: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/places/details', methods=['POST'])
    def get_place_details():
        """Fetch detailed info for selected places - PAID (Essential fields)"""
        try:
            data = request.json
            place_ids = data.get('place_ids', [])
            extract_emails = data.get('extract_emails', True)  # Auto-extract emails from websites
            
            if not place_ids:
                return jsonify({'error': 'No place IDs provided'}), 400
            
            google_api_key = get_api_key_from_db('google')
            
            if not google_api_key:
                return jsonify({'error': 'Google API key not configured'}), 400
            
            import requests
            results = []
            
            # Essential fields + contact info (website, phone)
            field_mask = 'id,displayName,formattedAddress,rating,userRatingCount,priceLevel,types,primaryType,location,websiteUri,nationalPhoneNumber,internationalPhoneNumber'
            
            for place_id in place_ids:
                try:
                    url = f"https://places.googleapis.com/v1/places/{place_id}"
                    headers = {
                        'X-Goog-Api-Key': google_api_key,
                        'X-Goog-FieldMask': field_mask
                    }
                    
                    res = requests.get(url, headers=headers, timeout=10)
                    
                    if res.status_code == 200:
                        place = res.json()
                        
                        price_map = {
                            'PRICE_LEVEL_FREE': 0,
                            'PRICE_LEVEL_INEXPENSIVE': 1,
                            'PRICE_LEVEL_MODERATE': 2,
                            'PRICE_LEVEL_EXPENSIVE': 3,
                            'PRICE_LEVEL_VERY_EXPENSIVE': 4
                        }
                        
                        website_url = place.get('websiteUri', '')
                        phone = place.get('internationalPhoneNumber') or place.get('nationalPhoneNumber', '')
                        
                        # Extract email from website if available
                        email = None
                        if extract_emails and website_url:
                            email = extract_email_from_website(website_url)
                        
                        results.append({
                            'place_id': place.get('id', place_id),
                            'name': place.get('displayName', {}).get('text', 'Unknown'),
                            'address': place.get('formattedAddress', ''),
                            'rating': place.get('rating', 0),
                            'review_count': place.get('userRatingCount', 0),
                            'price_level': price_map.get(place.get('priceLevel', ''), 2),
                            'types': place.get('types', []),
                            'primary_type': place.get('primaryType', 'restaurant'),
                            'latitude': place.get('location', {}).get('latitude', 0),
                            'longitude': place.get('location', {}).get('longitude', 0),
                            'website': website_url,
                            'phone': phone,
                            'email': email,
                            'has_details': True
                        })
                except Exception as e:
                    app.logger.warning(f"Failed to get details for {place_id}: {e}")
            
            # Track API usage
            if len(results) > 0:
                cost_per_place = 0.020  # Essential + Contact fields cost
                total_cost = len(results) * cost_per_place
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    if USE_POSTGRES:
                        cur.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                            VALUES (%s, %s, %s, %s, NOW())""",
                            ('google', 'Place Details + Contact', len(results), total_cost))
                    else:
                        cur.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                            VALUES (?, ?, ?, ?, datetime('now'))""",
                            ('google', 'Place Details + Contact', len(results), total_cost))
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as track_err:
                    app.logger.warning(f"Failed to track API usage: {track_err}")
            
            # Count emails found
            emails_found = sum(1 for r in results if r.get('email'))
            
            return jsonify({
                'success': True,
                'results': results,
                'fetched': len(results),
                'requested': len(place_ids),
                'emails_found': emails_found
            })
            
        except Exception as e:
            import traceback
            app.logger.error(f"Details error: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/places/quick-search', methods=['POST'])
    def quick_search_places():
        """Single-request search with full details and rating filter"""
        try:
            data = request.json
            city = data.get('city', '')
            country = data.get('country', 'Switzerland')
            min_rating = float(data.get('min_rating', 4.0))
            max_results = int(data.get('max_results', 20))
            
            if not city:
                return jsonify({'error': 'City is required'}), 400
            
            google_api_key = get_api_key_from_db('google')
            
            if not google_api_key:
                return jsonify({'error': 'Google API key not configured'}), 400
            
            import requests
            
            # Search with full details in one request
            query = f"best restaurants in {city}, {country}"
            url = "https://places.googleapis.com/v1/places:searchText"
            
            # Request full details + contact info
            field_mask = 'places.id,places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.priceLevel,places.types,places.primaryType,places.location,places.websiteUri,places.nationalPhoneNumber'
            
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': google_api_key,
                'X-Goog-FieldMask': field_mask
            }
            
            body = {
                'textQuery': query,
                'maxResultCount': min(max_results, 20)  # API limit per request
            }
            
            res = requests.post(url, json=body, headers=headers, timeout=15)
            
            results = []
            if res.status_code == 200:
                places_data = res.json()
                places = places_data.get('places', [])
                
                price_map = {
                    'PRICE_LEVEL_FREE': 0,
                    'PRICE_LEVEL_INEXPENSIVE': 1,
                    'PRICE_LEVEL_MODERATE': 2,
                    'PRICE_LEVEL_EXPENSIVE': 3,
                    'PRICE_LEVEL_VERY_EXPENSIVE': 4
                }
                
                for place in places:
                    rating = place.get('rating', 0)
                    
                    # Filter by minimum rating
                    if rating >= min_rating:
                        website_url = place.get('websiteUri', '')
                        phone = place.get('nationalPhoneNumber', '')
                        
                        # Try to extract email
                        email = None
                        if website_url:
                            try:
                                email = extract_email_from_website(website_url)
                            except:
                                pass
                        
                        results.append({
                            'place_id': place.get('id', ''),
                            'name': place.get('displayName', {}).get('text', 'Unknown'),
                            'address': place.get('formattedAddress', ''),
                            'rating': rating,
                            'review_count': place.get('userRatingCount', 0),
                            'price_level': price_map.get(place.get('priceLevel', ''), 2),
                            'types': place.get('types', []),
                            'primary_type': place.get('primaryType', 'restaurant'),
                            'latitude': place.get('location', {}).get('latitude', 0),
                            'longitude': place.get('location', {}).get('longitude', 0),
                            'website': website_url,
                            'phone': phone,
                            'email': email,
                            'has_details': True
                        })
            
            # Track API usage
            cost = 0.032 * len(results)  # Text Search + Full fields
            try:
                conn = get_db()
                cur = conn.cursor()
                if USE_POSTGRES:
                    cur.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                        VALUES (%s, %s, %s, %s, NOW())""",
                        ('google', 'Quick Search (Full Details)', len(results), cost))
                else:
                    cur.execute("""INSERT INTO api_usage (provider, endpoint, request_count, estimated_cost, timestamp)
                        VALUES (?, ?, ?, ?, datetime('now'))""",
                        ('google', 'Quick Search (Full Details)', len(results), cost))
                conn.commit()
                cur.close()
                conn.close()
            except:
                pass
            
            return jsonify({
                'success': True,
                'results': results,
                'total_found': len(results),
                'min_rating_filter': min_rating
            })
            
        except Exception as e:
            import traceback
            app.logger.error(f"Quick search error: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/places/import', methods=['POST'])
    def import_places():
        """Import selected restaurants from search results"""
        try:
            data = request.json
            restaurants = data.get('restaurants', [])
            city = data.get('city', '')
            country = data.get('country', 'Unknown')
            skip_duplicates = data.get('skip_duplicates', True)
            
            app.logger.info(f"Import request: {len(restaurants)} restaurants for {city}, {country}")
            
            if not restaurants:
                return jsonify({'error': 'No restaurants to import'}), 400
            
            # Log sample data to debug
            if restaurants:
                sample = restaurants[0]
                app.logger.info(f"Sample restaurant data: name={sample.get('name')}, rating={sample.get('rating')}, reviews={sample.get('review_count')}")
            
            conn = get_db()
            cur = get_cursor(conn)
            
            # Generate normalized region code from city (to prevent duplicates)
            # Normalize: lowercase, remove accents, remove spaces, take first 3 chars
            import unicodedata
            normalized_city = ''.join(
                c for c in unicodedata.normalize('NFD', city.lower())
                if unicodedata.category(c) != 'Mn'
            ).replace(' ', '').replace('-', '')
            region_code = normalized_city[:3].upper()
            
            app.logger.info(f"Region code: {region_code} for city: {city}")
            
            # Check if region already exists (by code OR by name)
            try:
                if USE_POSTGRES:
                    cur.execute("SELECT code FROM regions WHERE code=%s OR LOWER(name)=LOWER(%s)", (region_code, city))
                else:
                    cur.execute("SELECT code FROM regions WHERE code=? OR LOWER(name)=LOWER(?)", (region_code, city))
                
                existing = cur.fetchone()
                if existing:
                    # Use the existing region code
                    region_code = existing['code'] if hasattr(existing, 'keys') else existing[0]
                    app.logger.info(f"Using existing region: {region_code}")
                else:
                    # Get coords from first restaurant or default
                    lat = restaurants[0].get('latitude', 0) if restaurants else 0
                    lng = restaurants[0].get('longitude', 0) if restaurants else 0
                    
                    if USE_POSTGRES:
                        cur.execute("""INSERT INTO regions (code, name, country, latitude, longitude)
                            VALUES (%s, %s, %s, %s, %s) ON CONFLICT(code) DO NOTHING""",
                            (region_code, city, country, lat, lng))
                    else:
                        cur.execute("""INSERT OR IGNORE INTO regions (code, name, country, latitude, longitude)
                            VALUES (?, ?, ?, ?, ?)""", (region_code, city, country, lat, lng))
                conn.commit()
            except Exception as e:
                app.logger.warning(f"Region creation warning: {e}")
                try:
                    conn.rollback()
                except:
                    pass
            
            imported = 0
            skipped = 0
            duplicates_found = []
            errors = []
            
            for rest in restaurants:
                try:
                    # Skip if marked as duplicate and skip_duplicates is True
                    if skip_duplicates and rest.get('is_duplicate'):
                        skipped += 1
                        continue
                    
                    # Check for duplicate again (in case of race condition)
                    if USE_POSTGRES:
                        cur.execute("SELECT id, name FROM restaurants WHERE LOWER(name)=LOWER(%s) AND LOWER(city)=LOWER(%s) LIMIT 1",
                            (rest['name'], city))
                    else:
                        cur.execute("SELECT id, name FROM restaurants WHERE LOWER(name)=LOWER(?) AND LOWER(city)=LOWER(?) LIMIT 1",
                            (rest['name'], city))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        duplicates_found.append({
                            'name': rest['name'],
                            'existing_id': existing['id'] if isinstance(existing, dict) else existing[0]
                        })
                        skipped += 1
                        continue
                    
                    # Determine cuisine type from types
                    cuisine = 'Restaurant'
                    if 'types' in rest:
                        type_map = {
                            'italian_restaurant': 'Italian',
                            'french_restaurant': 'French',
                            'japanese_restaurant': 'Japanese',
                            'chinese_restaurant': 'Chinese',
                            'indian_restaurant': 'Indian',
                            'mexican_restaurant': 'Mexican',
                            'thai_restaurant': 'Thai',
                            'pizza_restaurant': 'Pizza',
                            'cafe': 'Café',
                            'bakery': 'Bakery',
                            'bar': 'Bar',
                            'meal_takeaway': 'Takeaway',
                            'fine_dining_restaurant': 'Fine Dining'
                        }
                        for t in rest.get('types', []):
                            if t in type_map:
                                cuisine = type_map[t]
                                break
                    
                    # Convert price level to text
                    price_map = {0: '$', 1: '$', 2: '$$', 3: '$$$', 4: '$$$$'}
                    price_range = price_map.get(rest.get('price_level', 2), '$$')
                    
                    # Insert restaurant
                    app.logger.debug(f"Inserting restaurant: {rest['name']}, city={city}, region_code={region_code}")
                    if USE_POSTGRES:
                        cur.execute("""INSERT INTO restaurants 
                            (name, address, city, canton, country, cuisine_type, price_range, 
                             latitude, longitude, phone, email, website, google_place_id, is_active)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING id""",
                            (rest['name'], rest.get('address', ''), city, region_code, country,
                             cuisine, price_range, rest.get('latitude', 0), rest.get('longitude', 0),
                             rest.get('phone', ''), rest.get('email', ''), rest.get('website', ''),
                             rest.get('place_id', ''), 1))
                        row = cur.fetchone()
                        rest_id = row['id'] if isinstance(row, dict) else row[0] if row else None
                    else:
                        cur.execute("""INSERT INTO restaurants 
                            (name, address, city, canton, country, cuisine_type, price_range, 
                             latitude, longitude, phone, email, website, google_place_id, is_active)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (rest['name'], rest.get('address', ''), city, region_code, country,
                             cuisine, price_range, rest.get('latitude', 0), rest.get('longitude', 0),
                             rest.get('phone', ''), rest.get('email', ''), rest.get('website', ''),
                             rest.get('place_id', ''), 1))
                        rest_id = cur.lastrowid
                    
                    app.logger.debug(f"Inserted restaurant ID: {rest_id}")
                    
                    if rest_id:
                        # Add source rating from Google
                        rating = rest.get('rating', 0)
                        reviews = rest.get('review_count', 0)
                        
                        if rating > 0:
                            try:
                                if USE_POSTGRES:
                                    cur.execute("""INSERT INTO source_ratings (restaurant_id, source, avg_rating, review_count)
                                        VALUES (%s, 'google', %s, %s)""", (rest_id, rating, reviews))
                                else:
                                    cur.execute("""INSERT INTO source_ratings (restaurant_id, source, avg_rating, review_count)
                                        VALUES (?, 'google', ?, ?)""", (rest_id, rating, reviews))
                            except:
                                pass
                        
                        imported += 1
                    
                    conn.commit()
                    
                except Exception as e:
                    app.logger.error(f"Error importing {rest.get('name', 'Unknown')}: {e}")
                    errors.append({'name': rest.get('name', 'Unknown'), 'error': str(e)[:50]})
                    try:
                        conn.rollback()
                    except:
                        pass
            
            app.logger.info(f"Import complete: {imported} imported, {skipped} skipped, {len(errors)} errors")
            
            # Compute rankings for the region
            try:
                cur.close()
                conn.close()
                app.logger.info(f"Computing rankings for region: {region_code}")
                compute_rankings(region_code)
                app.logger.info(f"Rankings computed successfully")
            except Exception as rank_err:
                app.logger.error(f"Error computing rankings: {rank_err}")
            
            return jsonify({
                'success': True,
                'imported': imported,
                'skipped': skipped,
                'duplicates': duplicates_found,
                'errors': errors,
                'region_code': region_code
            })
            
        except Exception as e:
            import traceback
            app.logger.error(f"Import error: {traceback.format_exc()}")
            try:
                conn.rollback()
            except:
                pass
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/debug/restaurants')
    def debug_restaurants():
        """Debug endpoint to check restaurant data"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Get recent restaurants
            cur.execute("""SELECT id, name, city, canton, country, is_active 
                FROM restaurants ORDER BY id DESC LIMIT 20""")
            restaurants = [dict_row(r) for r in cur.fetchall()]
            
            # Get all regions
            cur.execute("SELECT code, name, country FROM regions")
            regions = [dict_row(r) for r in cur.fetchall()]
            
            # Count per canton
            cur.execute("""SELECT canton, COUNT(*) as cnt FROM restaurants 
                WHERE is_active=1 GROUP BY canton""")
            canton_counts = [dict_row(r) for r in cur.fetchall()]
            
            # Count per city
            cur.execute("""SELECT city, COUNT(*) as cnt FROM restaurants 
                WHERE is_active=1 GROUP BY city""")
            city_counts = [dict_row(r) for r in cur.fetchall()]
            
            # Total count
            cur.execute("SELECT COUNT(*) as total FROM restaurants WHERE is_active=1")
            total = cur.fetchone()
            total_count = total['total'] if isinstance(total, dict) else total[0]
            
            cur.close()
            conn.close()
            
            return jsonify({
                'total_restaurants': total_count,
                'recent_restaurants': restaurants,
                'regions': regions,
                'by_canton': canton_counts,
                'by_city': city_counts
            })
        except Exception as e:
            import traceback
            return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500
    
    @app.route('/api/restaurants/extract-emails', methods=['POST'])
    def extract_emails_for_restaurants():
        """Extract emails from websites for restaurants that have website but no email"""
        try:
            data = request.json
            restaurant_ids = data.get('restaurant_ids', [])
            limit = data.get('limit', 50)  # Max restaurants to process
            
            conn = get_db()
            cur = get_cursor(conn)
            
            # Get restaurants with website but no email
            if restaurant_ids:
                if USE_POSTGRES:
                    placeholders = ','.join(['%s'] * len(restaurant_ids))
                    cur.execute(f"""SELECT id, name, website FROM restaurants 
                        WHERE id IN ({placeholders}) AND website IS NOT NULL AND website != '' 
                        AND (email IS NULL OR email = '')""", tuple(restaurant_ids))
                else:
                    placeholders = ','.join(['?'] * len(restaurant_ids))
                    cur.execute(f"""SELECT id, name, website FROM restaurants 
                        WHERE id IN ({placeholders}) AND website IS NOT NULL AND website != '' 
                        AND (email IS NULL OR email = '')""", tuple(restaurant_ids))
            else:
                if USE_POSTGRES:
                    cur.execute("""SELECT id, name, website FROM restaurants 
                        WHERE website IS NOT NULL AND website != '' 
                        AND (email IS NULL OR email = '')
                        LIMIT %s""", (limit,))
                else:
                    cur.execute("""SELECT id, name, website FROM restaurants 
                        WHERE website IS NOT NULL AND website != '' 
                        AND (email IS NULL OR email = '')
                        LIMIT ?""", (limit,))
            
            restaurants_to_process = [dict_row(r) for r in cur.fetchall()]
            
            results = {
                'processed': 0,
                'emails_found': 0,
                'failed': 0,
                'details': []
            }
            
            for rest in restaurants_to_process:
                try:
                    email = extract_email_from_website(rest['website'])
                    results['processed'] += 1
                    
                    if email:
                        # Update restaurant with email
                        if USE_POSTGRES:
                            cur.execute("UPDATE restaurants SET email = %s WHERE id = %s", (email, rest['id']))
                        else:
                            cur.execute("UPDATE restaurants SET email = ? WHERE id = ?", (email, rest['id']))
                        
                        results['emails_found'] += 1
                        results['details'].append({
                            'id': rest['id'],
                            'name': rest['name'],
                            'email': email,
                            'status': 'found'
                        })
                    else:
                        results['details'].append({
                            'id': rest['id'],
                            'name': rest['name'],
                            'email': None,
                            'status': 'not_found'
                        })
                        
                except Exception as e:
                    results['failed'] += 1
                    results['details'].append({
                        'id': rest['id'],
                        'name': rest['name'],
                        'error': str(e)[:50],
                        'status': 'error'
                    })
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({
                'success': True,
                **results
            })
            
        except Exception as e:
            import traceback
            app.logger.error(f"Email extraction error: {traceback.format_exc()}")
            return jsonify({'error': str(e)}), 500

    @app.route('/api/restaurants/check-duplicates', methods=['GET'])
    def check_duplicates():
        """Find duplicate restaurants in the database"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Find restaurants with same name in same city
            if USE_POSTGRES:
                cur.execute("""
                    SELECT name, city, country, COUNT(*) as count, 
                           ARRAY_AGG(id) as ids
                    FROM restaurants 
                    WHERE is_active=1
                    GROUP BY LOWER(name), LOWER(city), country
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                """)
            else:
                cur.execute("""
                    SELECT name, city, country, COUNT(*) as count, 
                           GROUP_CONCAT(id) as ids
                    FROM restaurants 
                    WHERE is_active=1
                    GROUP BY LOWER(name), LOWER(city), country
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                """)
            
            duplicates = []
            for row in cur.fetchall():
                r = dict_row(row) if USE_POSTGRES else {
                    'name': row[0], 'city': row[1], 'country': row[2], 
                    'count': row[3], 'ids': row[4]
                }
                duplicates.append(r)
            
            cur.close()
            conn.close()
            
            return jsonify({
                'total_duplicate_groups': len(duplicates),
                'duplicates': duplicates
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/scrape/jobs')
    def get_jobs():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT * FROM scrape_jobs ORDER BY created_at DESC LIMIT 30")
        jobs = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for j in jobs:
            if j:
                for k, v in j.items():
                    j[k] = dt_to_str(v)
        return jsonify(jobs)

    # ========== USER REVIEWS API ==========
    
    @app.route('/api/reviews/user', methods=['POST'])
    def add_user_review():
        data = request.json
        restaurant_id = data.get('restaurant_id')
        restaurant_name = data.get('restaurant_name')
        
        if not restaurant_id and not restaurant_name:
            return jsonify({'error': 'restaurant_id or restaurant_name required'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        p = '%s' if USE_POSTGRES else '?'
        
        if restaurant_id:
            cur.execute(f"SELECT id, canton FROM restaurants WHERE id={p}", (restaurant_id,))
        else:
            cur.execute(f"SELECT id, canton FROM restaurants WHERE name LIKE {p}", (f"%{restaurant_name}%",))
        
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return jsonify({'error': 'Restaurant not found'}), 404
        
        rest_id = row[0]
        region_code = row[1] if len(row) > 1 else None
        
        rating = data.get('rating', 3)
        review_text = data.get('review_text', '')
        
        # Analyze review text with NLP
        text_sentiment = analyze_review_sentiment(review_text)
        
        # Get text_sentiment weight from config
        text_weight = 0.5  # Default 50% text, 50% stars
        try:
            cur.execute("SELECT weight_text_sentiment FROM ranking_weights WHERE is_active=1 LIMIT 1")
            weight_row = cur.fetchone()
            if weight_row:
                text_weight = weight_row[0] if isinstance(weight_row, tuple) else weight_row.get('weight_text_sentiment', 0.5)
        except:
            pass
        
        # Combine star rating with text sentiment
        combined_sentiment = combine_rating_and_text_sentiment(rating, text_sentiment, text_weight)
        
        # Determine label from combined score
        if combined_sentiment > 0.2:
            sentiment_label = 'positive'
        elif combined_sentiment < -0.2:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'
        
        # Extract aspect scores
        aspects = text_sentiment.get('aspects', {})
        
        if USE_POSTGRES:
            cur.execute("""INSERT INTO reviews 
                (restaurant_id, source, rating, review_text, reviewer_name, sentiment_score, sentiment_label,
                 text_polarity, text_subjectivity, aspect_food, aspect_service, aspect_ambiance, aspect_value,
                 is_user_review, user_id, verified)
                VALUES (%s, 'user', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s) RETURNING id""",
                (rest_id, rating, review_text, data.get('reviewer_name', 'Anonymous'),
                 round(combined_sentiment, 3), sentiment_label,
                 text_sentiment.get('polarity'), text_sentiment.get('subjectivity'),
                 aspects.get('food'), aspects.get('service'), aspects.get('ambiance'), aspects.get('value'),
                 data.get('user_id'), data.get('verified', 0)))
            review_id = cur.fetchone()[0]
        else:
            cur.execute("""INSERT INTO reviews 
                (restaurant_id, source, rating, review_text, reviewer_name, sentiment_score, sentiment_label,
                 text_polarity, text_subjectivity, aspect_food, aspect_service, aspect_ambiance, aspect_value,
                 is_user_review, user_id, verified)
                VALUES (?, 'user', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                (rest_id, rating, review_text, data.get('reviewer_name', 'Anonymous'),
                 round(combined_sentiment, 3), sentiment_label,
                 text_sentiment.get('polarity'), text_sentiment.get('subjectivity'),
                 aspects.get('food'), aspects.get('service'), aspects.get('ambiance'), aspects.get('value'),
                 data.get('user_id'), data.get('verified', 0)))
            review_id = cur.lastrowid
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True, 
            'review_id': review_id, 
            'region_code': region_code,
            'sentiment': {
                'combined_score': combined_sentiment,
                'label': sentiment_label,
                'text_polarity': text_sentiment.get('polarity'),
                'text_subjectivity': text_sentiment.get('subjectivity'),
                'aspects': aspects
            }
        })

    # ========== SENTIMENT ANALYSIS ==========
    
    @app.route('/api/sentiment/analyze', methods=['POST'])
    def analyze_sentiment_endpoint():
        """Analyze sentiment of provided text - useful for testing"""
        data = request.json
        text = data.get('text', '')
        rating = data.get('rating')  # Optional star rating
        
        if not text:
            return jsonify({'error': 'No text provided'}), 400
        
        result = analyze_review_sentiment(text)
        
        if rating:
            # Get text weight from config
            try:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT weight_text_sentiment FROM ranking_weights WHERE is_active=1 LIMIT 1")
                row = cur.fetchone()
                text_weight = row[0] if row else 0.5
                cur.close()
                conn.close()
            except:
                text_weight = 0.5
            
            result['combined_score'] = combine_rating_and_text_sentiment(rating, result, text_weight)
            result['star_rating'] = rating
            result['text_weight_used'] = text_weight
        
        return jsonify(result)
    
    @app.route('/api/sentiment/reanalyze', methods=['POST'])
    def reanalyze_reviews_sentiment():
        """Reanalyze sentiment for existing reviews using NLP"""
        data = request.json
        restaurant_id = data.get('restaurant_id')  # Optional: only reanalyze for specific restaurant
        limit = data.get('limit', 100)
        
        conn = get_db()
        cur = get_cursor(conn)
        
        # Get text weight from config
        try:
            cur.execute("SELECT weight_text_sentiment FROM ranking_weights WHERE is_active=1 LIMIT 1")
            row = cur.fetchone()
            text_weight = row[0] if isinstance(row, tuple) else (row.get('weight_text_sentiment', 0.5) if row else 0.5)
        except:
            text_weight = 0.5
        
        # Get reviews with text to reanalyze
        if restaurant_id:
            p = '%s' if USE_POSTGRES else '?'
            cur.execute(f"""SELECT id, rating, review_text FROM reviews 
                WHERE restaurant_id={p} AND review_text IS NOT NULL AND review_text != ''
                ORDER BY created_at DESC LIMIT {limit}""", (restaurant_id,))
        else:
            cur.execute(f"""SELECT id, rating, review_text FROM reviews 
                WHERE review_text IS NOT NULL AND review_text != ''
                ORDER BY created_at DESC LIMIT {limit}""")
        
        reviews = [dict_row(r) for r in cur.fetchall()]
        
        updated = 0
        errors = 0
        
        for review in reviews:
            try:
                text_sentiment = analyze_review_sentiment(review['review_text'])
                combined = combine_rating_and_text_sentiment(
                    review['rating'] or 3, 
                    text_sentiment, 
                    text_weight
                )
                
                # Determine label
                if combined > 0.2:
                    label = 'positive'
                elif combined < -0.2:
                    label = 'negative'
                else:
                    label = 'neutral'
                
                aspects = text_sentiment.get('aspects', {})
                
                p = '%s' if USE_POSTGRES else '?'
                cur.execute(f"""UPDATE reviews SET 
                    sentiment_score={p}, sentiment_label={p},
                    text_polarity={p}, text_subjectivity={p},
                    aspect_food={p}, aspect_service={p}, aspect_ambiance={p}, aspect_value={p}
                    WHERE id={p}""",
                    (round(combined, 3), label,
                     text_sentiment.get('polarity'), text_sentiment.get('subjectivity'),
                     aspects.get('food'), aspects.get('service'), 
                     aspects.get('ambiance'), aspects.get('value'),
                     review['id']))
                
                updated += 1
                
            except Exception as e:
                errors += 1
                app.logger.warning(f"Failed to reanalyze review {review['id']}: {e}")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'updated': updated,
            'errors': errors,
            'total_processed': len(reviews),
            'text_weight_used': text_weight
        })
    
    @app.route('/api/sentiment/stats')
    def get_sentiment_stats():
        """Get sentiment statistics for dashboard"""
        restaurant_id = request.args.get('restaurant_id')
        
        conn = get_db()
        cur = get_cursor(conn)
        
        stats = {
            'total_reviews': 0,
            'with_text': 0,
            'positive': 0,
            'neutral': 0,
            'negative': 0,
            'avg_sentiment': 0,
            'avg_text_polarity': 0,
            'aspects': {
                'food': {'count': 0, 'avg': 0},
                'service': {'count': 0, 'avg': 0},
                'ambiance': {'count': 0, 'avg': 0},
                'value': {'count': 0, 'avg': 0}
            }
        }
        
        try:
            p = '%s' if USE_POSTGRES else '?'
            
            if restaurant_id:
                base_where = f"WHERE restaurant_id={p}"
                params = (restaurant_id,)
            else:
                base_where = ""
                params = ()
            
            # Total reviews
            cur.execute(f"SELECT COUNT(*) FROM reviews {base_where}", params)
            stats['total_reviews'] = cur.fetchone()[0]
            
            # Reviews with text
            where = f"{base_where} AND" if base_where else "WHERE"
            cur.execute(f"SELECT COUNT(*) FROM reviews {where} review_text IS NOT NULL AND review_text != ''", params)
            stats['with_text'] = cur.fetchone()[0]
            
            # Sentiment distribution
            cur.execute(f"SELECT sentiment_label, COUNT(*) FROM reviews {where} sentiment_label IS NOT NULL GROUP BY sentiment_label", params)
            for row in cur.fetchall():
                label = row[0]
                count = row[1]
                if label in stats:
                    stats[label] = count
            
            # Average sentiment
            cur.execute(f"SELECT AVG(sentiment_score) FROM reviews {where} sentiment_score IS NOT NULL", params)
            row = cur.fetchone()
            stats['avg_sentiment'] = round(row[0], 3) if row[0] else 0
            
            # Average text polarity
            cur.execute(f"SELECT AVG(text_polarity) FROM reviews {where} text_polarity IS NOT NULL", params)
            row = cur.fetchone()
            stats['avg_text_polarity'] = round(row[0], 3) if row[0] else 0
            
            # Aspect averages
            for aspect in ['food', 'service', 'ambiance', 'value']:
                cur.execute(f"SELECT COUNT(*), AVG(aspect_{aspect}) FROM reviews {where} aspect_{aspect} IS NOT NULL", params)
                row = cur.fetchone()
                if row:
                    stats['aspects'][aspect] = {
                        'count': row[0] or 0,
                        'avg': round(row[1], 2) if row[1] else 0
                    }
            
        except Exception as e:
            app.logger.error(f"Sentiment stats error: {e}")
        
        cur.close()
        conn.close()
        
        return jsonify(stats)

    # ========== SENTIMENT KEYWORDS ==========
    
    @app.route('/api/sentiment/keywords')
    def get_sentiment_keywords():
        """Get all custom sentiment keywords"""
        category = request.args.get('category')
        
        conn = get_db()
        cur = get_cursor(conn)
        
        rows = []
        try:
            if category:
                p = '%s' if USE_POSTGRES else '?'
                cur.execute(f"""SELECT * FROM sentiment_keywords WHERE is_active=1 AND category={p}
                    ORDER BY category, sentiment, keyword""", (category,))
            else:
                cur.execute("""SELECT * FROM sentiment_keywords WHERE is_active=1
                    ORDER BY category, sentiment, keyword""")
            
            rows = [dict_row(r) for r in cur.fetchall()]
        except Exception as e:
            # Table might not exist yet
            app.logger.warning(f"Keywords query failed (table may not exist): {e}")
            if USE_POSTGRES:
                try:
                    conn.rollback()
                except:
                    pass
        
        cur.close()
        conn.close()
        
        # Also return default keywords for reference
        default_keywords = {
            'food': {
                'positive': ['delicious', 'tasty', 'fresh', 'flavorful', 'amazing food', 'excellent food', 
                            'best food', 'great food', 'yummy', 'authentic', 'homemade'],
                'negative': ['bland', 'tasteless', 'cold', 'undercooked', 'overcooked', 'stale', 
                            'bad food', 'disgusting', 'burnt', 'soggy', 'greasy']
            },
            'service': {
                'positive': ['friendly', 'attentive', 'helpful', 'professional', 'fast service', 
                            'great service', 'polite', 'welcoming', 'efficient'],
                'negative': ['slow', 'rude', 'ignored', 'unfriendly', 'bad service', 
                            'poor service', 'inattentive', 'unprofessional']
            },
            'ambiance': {
                'positive': ['cozy', 'romantic', 'beautiful', 'great ambiance', 'clean',
                            'comfortable', 'charming', 'elegant', 'relaxing'],
                'negative': ['noisy', 'loud', 'dirty', 'cramped', 'smelly', 
                            'uncomfortable', 'crowded', 'run-down']
            },
            'value': {
                'positive': ['worth it', 'good value', 'reasonable', 'affordable', 'generous portions',
                            'great price', 'bargain'],
                'negative': ['overpriced', 'expensive', 'rip-off', 'tiny portions', 
                            'not worth', 'poor value']
            }
        }
        
        return jsonify({
            'custom_keywords': rows,
            'default_keywords': default_keywords,
            'categories': ['food', 'service', 'ambiance', 'value']
        })
    
    @app.route('/api/sentiment/keywords', methods=['POST'])
    def add_sentiment_keyword():
        """Add a new custom sentiment keyword"""
        data = request.json
        keyword = data.get('keyword', '').strip().lower()
        category = data.get('category', '').strip().lower()
        sentiment = data.get('sentiment', '').strip().lower()
        weight = data.get('weight', 1.0)
        language = data.get('language', 'en')
        
        if not keyword or not category or not sentiment:
            return jsonify({'error': 'keyword, category, and sentiment are required'}), 400
        
        if sentiment not in ['positive', 'negative']:
            return jsonify({'error': 'sentiment must be "positive" or "negative"'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        try:
            if USE_POSTGRES:
                cur.execute("""INSERT INTO sentiment_keywords (keyword, category, sentiment, weight, language)
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (keyword, category, language) DO UPDATE SET 
                    sentiment=EXCLUDED.sentiment, weight=EXCLUDED.weight, is_active=1
                    RETURNING id""",
                    (keyword, category, sentiment, weight, language))
                row = cur.fetchone()
                keyword_id = row[0] if row else None
            else:
                cur.execute("""INSERT OR REPLACE INTO sentiment_keywords 
                    (keyword, category, sentiment, weight, language, is_active)
                    VALUES (?, ?, ?, ?, ?, 1)""",
                    (keyword, category, sentiment, weight, language))
                keyword_id = cur.lastrowid
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'id': keyword_id,
                'keyword': keyword,
                'category': category,
                'sentiment': sentiment
            })
            
        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/sentiment/keywords/bulk', methods=['POST'])
    def add_sentiment_keywords_bulk():
        """Add multiple keywords at once"""
        data = request.json
        keywords = data.get('keywords', [])
        
        if not keywords:
            return jsonify({'error': 'No keywords provided'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        
        added = 0
        errors = []
        
        for kw in keywords:
            try:
                keyword = kw.get('keyword', '').strip().lower()
                category = kw.get('category', '').strip().lower()
                sentiment = kw.get('sentiment', '').strip().lower()
                weight = kw.get('weight', 1.0)
                language = kw.get('language', 'en')
                
                if not keyword or not category or sentiment not in ['positive', 'negative']:
                    errors.append({'keyword': keyword, 'error': 'Invalid data'})
                    continue
                
                if USE_POSTGRES:
                    cur.execute("""INSERT INTO sentiment_keywords (keyword, category, sentiment, weight, language)
                        VALUES (%s, %s, %s, %s, %s) 
                        ON CONFLICT (keyword, category, language) DO UPDATE SET 
                        sentiment=EXCLUDED.sentiment, weight=EXCLUDED.weight, is_active=1""",
                        (keyword, category, sentiment, weight, language))
                else:
                    cur.execute("""INSERT OR REPLACE INTO sentiment_keywords 
                        (keyword, category, sentiment, weight, language, is_active)
                        VALUES (?, ?, ?, ?, ?, 1)""",
                        (keyword, category, sentiment, weight, language))
                
                added += 1
                
            except Exception as e:
                errors.append({'keyword': kw.get('keyword', ''), 'error': str(e)[:50]})
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'added': added,
            'errors': errors
        })
    
    @app.route('/api/sentiment/keywords/<int:keyword_id>', methods=['DELETE'])
    def delete_sentiment_keyword(keyword_id):
        """Delete (deactivate) a custom sentiment keyword"""
        conn = get_db()
        cur = conn.cursor()
        
        p = '%s' if USE_POSTGRES else '?'
        cur.execute(f"UPDATE sentiment_keywords SET is_active=0 WHERE id={p}", (keyword_id,))
        
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'deleted': affected > 0})
    
    @app.route('/api/sentiment/keywords/category/<category>', methods=['DELETE'])
    def delete_keywords_by_category(category):
        """Delete all custom keywords in a category"""
        conn = get_db()
        cur = conn.cursor()
        
        p = '%s' if USE_POSTGRES else '?'
        cur.execute(f"UPDATE sentiment_keywords SET is_active=0 WHERE category={p}", (category.lower(),))
        
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'deleted': affected})

    # ========== RESTAURANTS ==========
    
    @app.route('/api/restaurants')
    def get_restaurants():
        region = request.args.get('region')
        needs_email = request.args.get('needs_email')
        
        conn = get_db()
        cur = get_cursor(conn)
        
        # Only select columns that exist in production schema
        sql = """SELECT r.*, rk.composite_score, rk.auto_rank, rk.manual_rank,
            rk.is_featured, rk.is_published, rk.total_reviews,
            rk.sentiment_avg, rk.confidence_score
            FROM restaurants r 
            LEFT JOIN rankings rk ON r.id=rk.restaurant_id
            WHERE r.is_active=1"""
        
        params = []
        
        if needs_email:
            # Return only restaurants with website but no email
            sql += " AND r.website IS NOT NULL AND r.website != '' AND (r.email IS NULL OR r.email = '')"
        
        if region:
            p = '%s' if USE_POSTGRES else '?'
            # Match by canton or city (case-insensitive)
            sql += f" AND (r.canton={p} OR LOWER(r.canton)=LOWER({p}) OR LOWER(r.city)=LOWER({p}))"
            params = [region, region, region]
        
        if params:
            cur.execute(sql + " ORDER BY COALESCE(rk.manual_rank, rk.auto_rank, 999)", tuple(params))
        else:
            cur.execute(sql + " ORDER BY COALESCE(rk.manual_rank, rk.auto_rank, 999)")
        
        rows = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r:
                for k, v in r.items():
                    r[k] = dt_to_str(v)
        return jsonify(rows)
    
    @app.route('/api/restaurants/<int:rid>')
    def get_restaurant(rid):
        conn = get_db()
        cur = get_cursor(conn)
        p = '%s' if USE_POSTGRES else '?'
        cur.execute(f"SELECT * FROM restaurants WHERE id={p}", (rid,))
        r = cur.fetchone()
        if not r:
            cur.close()
            conn.close()
            return jsonify({'error': 'Not found'}), 404
        
        rest = dict_row(r)
        cur.execute(f"SELECT * FROM rankings WHERE restaurant_id={p}", (rid,))
        rk = cur.fetchone()
        rest['ranking'] = dict_row(rk)
        cur.execute(f"SELECT * FROM source_ratings WHERE restaurant_id={p}", (rid,))
        rest['source_ratings'] = [dict_row(x) for x in cur.fetchall()]
        cur.execute(f"SELECT * FROM reviews WHERE restaurant_id={p} ORDER BY created_at DESC LIMIT 50", (rid,))
        rest['reviews'] = [dict_row(x) for x in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify(rest)

    @app.route('/api/restaurants/<int:rid>/exclude', methods=['POST'])
    def exclude_restaurant(rid):
        """Exclude a restaurant from rankings"""
        try:
            data = request.json or {}
            reason = data.get('reason', '')
            
            conn = get_db()
            cur = conn.cursor()
            p = '%s' if USE_POSTGRES else '?'
            
            cur.execute(f"UPDATE restaurants SET is_excluded=1, exclude_reason={p} WHERE id={p}", (reason, rid))
            
            # Also unpublish from rankings
            cur.execute(f"UPDATE rankings SET is_published=0 WHERE restaurant_id={p}", (rid,))
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'message': 'Restaurant excluded'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.route('/api/restaurants/<int:rid>/include', methods=['POST'])
    def include_restaurant(rid):
        """Re-include a previously excluded restaurant"""
        try:
            conn = get_db()
            cur = conn.cursor()
            p = '%s' if USE_POSTGRES else '?'
            
            cur.execute(f"UPDATE restaurants SET is_excluded=0, exclude_reason=NULL WHERE id={p}", (rid,))
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'message': 'Restaurant included'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500

    # ========== RANKINGS ==========
    
    @app.route('/api/rankings')
    def get_rankings():
        region = request.args.get('region')
        limit = request.args.get('limit', type=int)
        conn = get_db()
        cur = get_cursor(conn)
        
        # Use canton from restaurants table (region_code doesn't exist in production)
        # Include sentiment_avg as sentiment_score and is_excluded
        sql = """SELECT r.id, r.name, r.cuisine_type as cuisine, r.price_range, r.city, r.canton as region_code,
            r.is_excluded, r.exclude_reason,
            rk.composite_score, rk.sentiment_avg as sentiment_score, rk.confidence_score,
            rk.total_reviews, rk.auto_rank, rk.manual_rank,
            rk.is_featured, rk.is_published, rk.admin_notes,
            (SELECT avg_rating FROM source_ratings sr WHERE sr.restaurant_id=r.id AND sr.source='google' LIMIT 1) as google_rating,
            (SELECT review_count FROM source_ratings sr WHERE sr.restaurant_id=r.id AND sr.source='google' LIMIT 1) as review_count
            FROM restaurants r 
            JOIN rankings rk ON r.id=rk.restaurant_id
            WHERE r.is_active=1"""
        
        if region:
            p = '%s' if USE_POSTGRES else '?'
            # Filter by restaurants.canton since rankings has no region column
            sql += f" AND (r.canton={p} OR LOWER(r.canton)=LOWER({p}) OR LOWER(r.city)=LOWER({p}))"
            sql += " ORDER BY COALESCE(rk.manual_rank, rk.auto_rank)"
            if limit:
                sql += f" LIMIT {int(limit)}"
            cur.execute(sql, (region, region, region))
        else:
            sql += " ORDER BY r.canton, COALESCE(rk.manual_rank, rk.auto_rank)"
            if limit:
                sql += f" LIMIT {int(limit)}"
            cur.execute(sql)
        
        rows = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify(rows)
    
    @app.route('/api/rankings/compute', methods=['POST'])
    def trigger_compute():
        data = request.json or {}
        region = data.get('region')
        
        if region:
            compute_rankings(region)
        else:
            # Compute for all regions
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT code FROM regions")
            regions = [r[0] for r in cur.fetchall()]
            cur.close()
            conn.close()
            for reg in regions:
                compute_rankings(reg)
        
        return jsonify({'success': True})
    
    @app.route('/api/rankings/<int:rid>', methods=['PUT'])
    def update_ranking(rid):
        data = request.json
        conn = get_db()
        cur = conn.cursor()
        
        if USE_POSTGRES:
            cur.execute("""UPDATE rankings SET 
                manual_rank=COALESCE(%s, manual_rank),
                is_featured=COALESCE(%s, is_featured), 
                is_published=COALESCE(%s, is_published),
                admin_notes=COALESCE(%s, admin_notes) 
                WHERE restaurant_id=%s""",
                (data.get('manual_rank'), data.get('is_featured'), data.get('is_published'),
                 data.get('admin_notes'), rid))
        else:
            cur.execute("""UPDATE rankings SET 
                manual_rank=COALESCE(?, manual_rank),
                is_featured=COALESCE(?, is_featured), 
                is_published=COALESCE(?, is_published),
                admin_notes=COALESCE(?, admin_notes) 
                WHERE restaurant_id=?""",
                (data.get('manual_rank'), data.get('is_featured'), data.get('is_published'),
                 data.get('admin_notes'), rid))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    
    @app.route('/api/rankings/publish', methods=['POST'])
    def publish_rankings():
        data = request.json
        region = data.get('region')
        top_n = data.get('top_n', 20)
        restaurant_ids = data.get('restaurant_ids')  # Specific selection
        
        if not region:
            return jsonify({'error': 'Region required'}), 400
        
        conn = get_db()
        cur = conn.cursor()
        p = '%s' if USE_POSTGRES else '?'
        
        # Unpublish all in region first - join with restaurants to filter by canton
        if USE_POSTGRES:
            cur.execute("""UPDATE rankings SET is_published=0 
                WHERE restaurant_id IN (SELECT id FROM restaurants WHERE canton=%s OR LOWER(canton)=LOWER(%s) OR LOWER(city)=LOWER(%s))""", 
                (region, region, region))
        else:
            cur.execute("""UPDATE rankings SET is_published=0 
                WHERE restaurant_id IN (SELECT id FROM restaurants WHERE canton=? OR LOWER(canton)=LOWER(?) OR LOWER(city)=LOWER(?))""", 
                (region, region, region))
        
        if restaurant_ids:
            # Publish specific restaurants
            placeholders = ','.join([p] * len(restaurant_ids))
            cur.execute(f"UPDATE rankings SET is_published=1 WHERE restaurant_id IN ({placeholders})",
                tuple(restaurant_ids))
            published = len(restaurant_ids)
        else:
            # Publish top N in this region
            if USE_POSTGRES:
                cur.execute("""UPDATE rankings SET is_published=1 WHERE restaurant_id IN (
                    SELECT rk.restaurant_id FROM rankings rk
                    JOIN restaurants r ON rk.restaurant_id = r.id
                    WHERE r.canton=%s OR LOWER(r.canton)=LOWER(%s) OR LOWER(r.city)=LOWER(%s)
                    ORDER BY COALESCE(rk.manual_rank, rk.auto_rank) LIMIT %s)""", (region, region, region, top_n))
            else:
                cur.execute("""UPDATE rankings SET is_published=1 WHERE restaurant_id IN (
                    SELECT rk.restaurant_id FROM rankings rk
                    JOIN restaurants r ON rk.restaurant_id = r.id
                    WHERE r.canton=? OR LOWER(r.canton)=LOWER(?) OR LOWER(r.city)=LOWER(?)
                    ORDER BY COALESCE(rk.manual_rank, rk.auto_rank) LIMIT ?)""", (region, region, region, top_n))
            published = top_n
        
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'published': published, 'region': region})

    # ========== SCHEMA ANALYZER ==========
    
    @app.route('/api/schema/test', methods=['POST'])
    def test_schema_endpoint():
        """Test connection to external API"""
        import requests
        data = request.json
        url = data.get('url', '')
        auth = data.get('auth', '')
        
        if not url:
            return jsonify({'success': False, 'error': 'URL required'})
        
        try:
            headers = {}
            if auth:
                headers['Authorization'] = auth
            
            res = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            return jsonify({
                'success': True,
                'status_code': res.status_code,
                'headers': dict(res.headers)
            })
        except requests.exceptions.Timeout:
            return jsonify({'success': False, 'error': 'Connection timed out'})
        except requests.exceptions.ConnectionError as e:
            return jsonify({'success': False, 'error': f'Connection failed: {str(e)}'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/schema/analyze-url', methods=['POST'])
    def analyze_url_simple():
        """Simple URL analysis - just paste URL and get structure"""
        import requests as req
        try:
            from bs4 import BeautifulSoup
            HAS_BS4 = True
        except ImportError:
            HAS_BS4 = False
        import re
        
        data = request.json
        url = data.get('url', '').strip()
        
        if not url:
            return jsonify({'success': False, 'error': 'URL required'})
        
        try:
            # Try to fetch the URL
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/html, */*'
            }
            
            res = req.get(url, headers=headers, timeout=15)
            
            fields = []
            sample_values = {}
            records_count = 0
            data_type = 'unknown'
            raw_data = None
            
            content_type = res.headers.get('content-type', '')
            
            # Try JSON first
            if 'json' in content_type or res.text.strip().startswith('{') or res.text.strip().startswith('['):
                try:
                    json_data = res.json()
                    raw_data = json_data
                    
                    # Find the data array
                    items = []
                    if isinstance(json_data, list):
                        items = json_data
                        data_type = 'JSON Array'
                    elif isinstance(json_data, dict):
                        # Look for common data wrapper keys
                        for key in ['data', 'results', 'items', 'restaurants', 'records', 'rows', 'entries', 'list']:
                            if key in json_data and isinstance(json_data[key], list):
                                items = json_data[key]
                                data_type = f'JSON Object (data in "{key}")'
                                break
                        if not items and json_data:
                            # Maybe it's a single record
                            items = [json_data]
                            data_type = 'JSON Object (single record)'
                    
                    records_count = len(items)
                    
                    # Extract fields from first few items
                    if items:
                        for item in items[:5]:
                            if isinstance(item, dict):
                                for key, value in item.items():
                                    if key not in fields:
                                        fields.append(key)
                                        sample_values[key] = value
                    
                except ValueError:
                    pass
            
            # Try HTML if not JSON
            if not fields and 'html' in content_type and HAS_BS4:
                try:
                    soup = BeautifulSoup(res.text, 'html.parser')
                    data_type = 'HTML Page'
                    
                    # Look for structured data (JSON-LD)
                    for script in soup.find_all('script', type='application/ld+json'):
                        try:
                            ld_data = json.loads(script.string)
                            if isinstance(ld_data, dict):
                                fields = list(ld_data.keys())
                                sample_values = {k: v for k, v in ld_data.items() if not isinstance(v, (dict, list))}
                                raw_data = ld_data
                                data_type = 'HTML with JSON-LD'
                                records_count = 1
                                break
                        except:
                            continue
                    
                    # Look for data tables
                    if not fields:
                        table = soup.find('table')
                        if table:
                            headers = table.find_all('th')
                            if headers:
                                fields = [th.get_text(strip=True) for th in headers]
                                data_type = 'HTML Table'
                                records_count = len(table.find_all('tr')) - 1
                    
                    # Look for restaurant cards/items with common classes
                    if not fields:
                        for class_pattern in ['restaurant', 'listing', 'card', 'item', 'result']:
                            items = soup.find_all(class_=re.compile(class_pattern, re.I))
                            if items:
                                records_count = len(items)
                                data_type = f'HTML ({len(items)} items found)'
                                break
                    
                except Exception as e:
                    app.logger.warning(f"HTML parsing error: {e}")
            
            if not fields:
                return jsonify({
                    'success': False,
                    'error': 'Could not detect data structure. Try a direct API endpoint that returns JSON.',
                    'content_type': content_type,
                    'response_preview': res.text[:500]
                })
            
            return jsonify({
                'success': True,
                'data_type': data_type,
                'fields': fields,
                'fields_count': len(fields),
                'records_count': records_count,
                'sample_values': {k: str(v)[:100] if v else None for k, v in sample_values.items()},
                'raw_data': raw_data[:3] if isinstance(raw_data, list) else raw_data
            })
            
        except req.exceptions.Timeout:
            return jsonify({'success': False, 'error': 'Request timed out'})
        except req.exceptions.ConnectionError:
            return jsonify({'success': False, 'error': 'Could not connect to URL'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/schema/analyze', methods=['POST'])
    def analyze_schema():
        """Analyze external API to detect schema"""
        import requests
        data = request.json
        url = data.get('url', '')
        method = data.get('method', 'GET')
        auth = data.get('auth', '')
        custom_headers = data.get('custom_headers', {})
        
        if not url:
            return jsonify({'success': False, 'error': 'URL required'})
        
        try:
            headers = {'Content-Type': 'application/json'}
            headers.update(custom_headers)
            if auth:
                headers['Authorization'] = auth
            
            # Make request based on method
            if method == 'GET':
                res = requests.get(url, headers=headers, timeout=15)
            elif method == 'OPTIONS':
                res = requests.options(url, headers=headers, timeout=15)
            elif method == 'POST':
                res = requests.post(url, headers=headers, json={}, timeout=15)
            else:
                res = requests.get(url, headers=headers, timeout=15)
            
            schema = {}
            suggested_mappings = {}
            
            # Try to parse JSON response
            try:
                response_data = res.json()
                
                # Handle different response structures
                items = []
                if isinstance(response_data, list):
                    items = response_data[:5]  # Take first 5 items
                elif isinstance(response_data, dict):
                    # Common wrapper patterns
                    for key in ['data', 'results', 'items', 'restaurants', 'records']:
                        if key in response_data and isinstance(response_data[key], list):
                            items = response_data[key][:5]
                            break
                    if not items:
                        items = [response_data]
                
                # Analyze schema from items
                if items:
                    for item in items:
                        if isinstance(item, dict):
                            for key, value in item.items():
                                if key not in schema:
                                    schema[key] = {
                                        'type': type(value).__name__,
                                        'example': str(value)[:100] if value else None,
                                        'nullable': value is None
                                    }
                
                # Suggest mappings based on field names
                mapping_hints = {
                    'name': ['name', 'title', 'restaurant_name', 'restaurantName', 'nom', 'label'],
                    'address': ['address', 'street', 'location', 'adresse', 'addr'],
                    'city': ['city', 'ville', 'town', 'locality', 'area'],
                    'latitude': ['latitude', 'lat', 'geo_lat', 'y'],
                    'longitude': ['longitude', 'lng', 'lon', 'geo_lng', 'x'],
                    'cuisine_type': ['cuisine', 'cuisine_type', 'cuisineType', 'type', 'category', 'food_type'],
                    'price_range': ['price', 'price_range', 'priceRange', 'price_level', 'cost'],
                    'google_rating': ['rating', 'google_rating', 'googleRating', 'score', 'stars'],
                    'google_reviews': ['reviews', 'review_count', 'reviewCount', 'num_reviews'],
                    'phone': ['phone', 'telephone', 'tel', 'phone_number', 'contact'],
                    'website': ['website', 'url', 'web', 'site', 'homepage'],
                    'image_url': ['image', 'image_url', 'imageUrl', 'photo', 'thumbnail', 'picture']
                }
                
                schema_keys_lower = {k.lower(): k for k in schema.keys()}
                
                for spotwego_field, possible_targets in mapping_hints.items():
                    for target in possible_targets:
                        if target.lower() in schema_keys_lower:
                            suggested_mappings[spotwego_field] = schema_keys_lower[target.lower()]
                            break
                
            except ValueError:
                # Not JSON, try to parse as text or check validation errors
                if res.status_code in [400, 422]:
                    # Validation error might contain schema info
                    schema = {'_error': res.text[:500], '_note': 'Check validation errors for field hints'}
            
            return jsonify({
                'success': True,
                'status_code': res.status_code,
                'schema': schema,
                'suggested_mappings': suggested_mappings,
                'raw_response_preview': res.text[:1000] if res.text else None
            })
            
        except requests.exceptions.Timeout:
            return jsonify({'success': False, 'error': 'Request timed out'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/schema/mapping', methods=['GET'])
    def get_schema_mapping():
        """Get saved field mapping"""
        conn = get_db()
        cur = get_cursor(conn)
        
        try:
            cur.execute("SELECT mapping_json FROM website_config LIMIT 1")
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row and row[0]:
                import json
                return jsonify({'success': True, 'mapping': json.loads(row[0])})
            return jsonify({'success': True, 'mapping': {}})
        except:
            cur.close()
            conn.close()
            return jsonify({'success': True, 'mapping': {}})
    
    @app.route('/api/schema/mapping', methods=['POST'])
    def save_schema_mapping():
        """Save field mapping configuration"""
        import json
        data = request.json
        mapping = data.get('mapping', {})
        
        conn = get_db()
        cur = conn.cursor()
        
        try:
            mapping_json = json.dumps(mapping)
            
            cur.execute("SELECT id FROM website_config LIMIT 1")
            if cur.fetchone():
                if USE_POSTGRES:
                    cur.execute("UPDATE website_config SET mapping_json = %s", (mapping_json,))
                else:
                    cur.execute("UPDATE website_config SET mapping_json = ?", (mapping_json,))
            else:
                if USE_POSTGRES:
                    cur.execute("INSERT INTO website_config (mapping_json) VALUES (%s)", (mapping_json,))
                else:
                    cur.execute("INSERT INTO website_config (mapping_json) VALUES (?)", (mapping_json,))
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/schema/preview', methods=['GET'])
    def preview_schema_data():
        """Get sample restaurants for mapping preview"""
        limit = request.args.get('limit', 2, type=int)
        
        conn = get_db()
        cur = get_cursor(conn)
        
        try:
            cur.execute(f"""
                SELECT r.*, rk.composite_score, rk.auto_rank, rk.manual_rank, rk.is_published, rk.is_featured
                FROM restaurants r
                LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                LIMIT {limit}
            """)
            
            restaurants = []
            for row in cur.fetchall():
                r = dict_row(row)
                if r:
                    for k, v in r.items():
                        r[k] = dt_to_str(v)
                    restaurants.append(r)
            
            cur.close()
            conn.close()
            return jsonify({'success': True, 'sample_restaurants': restaurants})
        except Exception as e:
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': str(e)})

    # ========== WEBSITE CONFIG & PUSH ==========
    
    @app.route('/api/website/config', methods=['GET'])
    def get_website_config():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT * FROM website_config LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            cfg = dict_row(row)
            for k, v in cfg.items():
                cfg[k] = dt_to_str(v)
            return jsonify(cfg)
        
        return jsonify({'api_url': '', 'push_endpoint': '/api/restaurants/import', 'auth_type': 'bearer'})
    
    @app.route('/api/website/config', methods=['POST'])
    def save_website_config():
        data = request.json
        api_key = data.get('api_key', '')
        encrypted = hashlib.sha256(api_key.encode()).hexdigest()[:16] + '...' + api_key[-4:] if api_key else ''
        
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("SELECT id FROM website_config LIMIT 1")
        exists = cur.fetchone()
        
        if USE_POSTGRES:
            if exists:
                cur.execute("""UPDATE website_config SET
                    api_url=%s, api_key_encrypted=%s, push_endpoint=%s, auth_type=%s, updated_at=NOW()
                    WHERE id=%s""",
                    (data.get('api_url'), encrypted, data.get('push_endpoint', '/api/restaurants/import'),
                     data.get('auth_type', 'bearer'), exists[0]))
            else:
                cur.execute("""INSERT INTO website_config (api_url, api_key_encrypted, push_endpoint, auth_type)
                    VALUES (%s, %s, %s, %s)""",
                    (data.get('api_url'), encrypted, data.get('push_endpoint'), data.get('auth_type')))
        else:
            if exists:
                cur.execute("""UPDATE website_config SET
                    api_url=?, api_key_encrypted=?, push_endpoint=?, auth_type=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?""",
                    (data.get('api_url'), encrypted, data.get('push_endpoint', '/api/restaurants/import'),
                     data.get('auth_type', 'bearer'), exists[0]))
            else:
                cur.execute("""INSERT INTO website_config (api_url, api_key_encrypted, push_endpoint, auth_type)
                    VALUES (?, ?, ?, ?)""",
                    (data.get('api_url'), encrypted, data.get('push_endpoint'), data.get('auth_type')))
        
        conn.commit()
        cur.close()
        conn.close()
        
        cfg = load_config()
        cfg['website'] = {'api_url': data.get('api_url'), 'api_key': api_key}
        save_config(cfg)
        
        return jsonify({'success': True})
    
    @app.route('/api/website/push', methods=['POST'])
    def push_to_website():
        data = request.json
        region = data.get('region')
        top_n = data.get('top_n', 20)
        restaurant_ids = data.get('restaurant_ids')
        
        if not region:
            return jsonify({'error': 'Region required'}), 400
        
        cfg = load_config()
        website_cfg = cfg.get('website', {})
        api_url = website_cfg.get('api_url')
        api_key = website_cfg.get('api_key')
        
        if not api_url:
            return jsonify({'error': 'Website API URL not configured'}), 400
        
        conn = get_db()
        cur = get_cursor(conn)
        p = '%s' if USE_POSTGRES else '?'
        
        # Get restaurants to push - join with restaurants to filter by region
        if restaurant_ids:
            placeholders = ','.join([p] * len(restaurant_ids))
            cur.execute(f"""SELECT r.*, rk.composite_score, rk.auto_rank, rk.manual_rank,
                rk.sentiment_avg, rk.confidence_score, rk.total_reviews
                FROM restaurants r
                JOIN rankings rk ON r.id = rk.restaurant_id
                WHERE r.id IN ({placeholders})
                ORDER BY COALESCE(rk.manual_rank, rk.auto_rank)""", tuple(restaurant_ids))
        else:
            # Filter by region via restaurants.canton or city
            if USE_POSTGRES:
                cur.execute("""SELECT r.*, rk.composite_score, rk.auto_rank, rk.manual_rank,
                    rk.sentiment_avg, rk.confidence_score, rk.total_reviews
                    FROM restaurants r
                    JOIN rankings rk ON r.id = rk.restaurant_id
                    WHERE rk.is_published = 1 AND (r.canton=%s OR LOWER(r.canton)=LOWER(%s) OR LOWER(r.city)=LOWER(%s))
                    ORDER BY COALESCE(rk.manual_rank, rk.auto_rank)
                    LIMIT %s""", (region, region, region, top_n))
            else:
                cur.execute("""SELECT r.*, rk.composite_score, rk.auto_rank, rk.manual_rank,
                    rk.sentiment_avg, rk.confidence_score, rk.total_reviews
                    FROM restaurants r
                    JOIN rankings rk ON r.id = rk.restaurant_id
                    WHERE rk.is_published = 1 AND (r.canton=? OR LOWER(r.canton)=LOWER(?) OR LOWER(r.city)=LOWER(?))
                    ORDER BY COALESCE(rk.manual_rank, rk.auto_rank)
                    LIMIT ?""", (region, region, region, top_n))
        
        restaurants = [dict_row(r) for r in cur.fetchall()]
        
        for r in restaurants:
            if r:
                for k, v in r.items():
                    r[k] = dt_to_str(v)
        
        # Load field mapping
        cur.execute("SELECT mapping_json FROM website_config LIMIT 1")
        mapping_row = cur.fetchone()
        field_mapping = {}
        if mapping_row:
            mapping_json = mapping_row['mapping_json'] if USE_POSTGRES else mapping_row[0]
            if mapping_json:
                import json
                try:
                    field_mapping = json.loads(mapping_json)
                except:
                    pass
        
        # Apply field mapping to restaurants
        mapped_restaurants = []
        for r in restaurants:
            if field_mapping:
                mapped = {}
                for spotwego_field, config in field_mapping.items():
                    if config.get('include', True) and spotwego_field in r:
                        target_field = config.get('target', spotwego_field)
                        value = r[spotwego_field]
                        
                        # Apply transform
                        transform = config.get('transform', 'none')
                        if transform == 'string' and value is not None:
                            value = str(value)
                        elif transform == 'number' and value is not None:
                            try:
                                value = float(value)
                            except:
                                pass
                        elif transform == 'boolean':
                            value = bool(value)
                        elif transform == 'uppercase' and value is not None:
                            value = str(value).upper()
                        elif transform == 'lowercase' and value is not None:
                            value = str(value).lower()
                        
                        mapped[target_field] = value
                mapped_restaurants.append(mapped)
            else:
                mapped_restaurants.append(r)
        
        payload = {
            'region': region,
            'restaurants': mapped_restaurants,
            'pushed_at': datetime.now().isoformat(),
            'count': len(mapped_restaurants)
        }
        
        try:
            import requests as req
            headers = {'Content-Type': 'application/json'}
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'
            
            cur.execute("SELECT push_endpoint FROM website_config LIMIT 1")
            row = cur.fetchone()
            endpoint = row['push_endpoint'] if row and USE_POSTGRES else (row[0] if row else '/api/restaurants/import')
            
            full_url = api_url.rstrip('/') + endpoint
            response = req.post(full_url, json=payload, headers=headers, timeout=30)
            
            status = 'success' if response.status_code == 200 else 'failed'
            
            # Log push - push_history doesn't have region_code column
            if USE_POSTGRES:
                cur.execute("""INSERT INTO push_history (restaurants_pushed, status, response_code, response_message, pushed_at)
                    VALUES (%s, %s, %s, %s, NOW())""",
                    (len(restaurants), status, response.status_code, response.text[:500] if response.text else ''))
            else:
                cur.execute("""INSERT INTO push_history (restaurants_pushed, status, response_code, response_message, pushed_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
                    (len(restaurants), status, response.status_code, response.text[:500] if response.text else ''))
            
            conn.commit()
            cur.close()
            conn.close()
            
            if response.status_code == 200:
                # Note: Welcome email functionality disabled - restaurants table 
                # doesn't have email/welcome_email_sent columns in production
                cur.close()
                conn.close()
                
                return jsonify({
                    'success': True, 
                    'pushed': len(restaurants), 
                    'region': region,
                    'welcome_emails_sent': emails_sent
                })
            else:
                cur.close()
                conn.close()
                return jsonify({'error': f'API returned {response.status_code}', 'message': response.text}), 400
            
        except Exception as e:
            if USE_POSTGRES:
                cur.execute("""INSERT INTO push_history (region_code, restaurants_pushed, status, response_message)
                    VALUES (%s, 0, 'error', %s)""", (region, str(e)))
            else:
                cur.execute("""INSERT INTO push_history (region_code, restaurants_pushed, status, response_message)
                    VALUES (?, 0, 'error', ?)""", (region, str(e)))
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/website/push/history')
    def get_push_history():
        conn = get_db()
        cur = get_cursor(conn)
        cur.execute("SELECT * FROM push_history ORDER BY pushed_at DESC LIMIT 30")
        rows = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        for r in rows:
            if r:
                for k, v in r.items():
                    r[k] = dt_to_str(v)
        return jsonify(rows)

    # ========== DAILY AUTOMATION ==========
    
    @app.route('/api/daily/run', methods=['POST'])
    def run_daily_tasks():
        """
        Optimized daily task - only recomputes restaurants with new reviews.
        Much more efficient at scale (thousands of restaurants).
        """
        results = {
            'restaurants_updated': 0,
            'regions_reranked': [],
            'regions_pushed': [],
            'skipped_no_changes': [],
            'errors': []
        }
        
        conn = get_db()
        cur = get_cursor(conn)
        p = '%s' if USE_POSTGRES else '?'
        
        # Step 1: Find restaurants with NEW reviews since last ranking computation
        # Use canton field since restaurants doesn't have region_code
        # Rankings are per-restaurant (not per-region) so simple join
        if USE_POSTGRES:
            cur.execute("""
                SELECT DISTINCT r.id, r.canton as region_code, r.city
                FROM restaurants r
                JOIN reviews rev ON r.id = rev.restaurant_id
                LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                WHERE rev.created_at > COALESCE(rk.last_computed, '1970-01-01')
                AND r.is_active = 1
            """)
        else:
            cur.execute("""
                SELECT DISTINCT r.id, r.canton as region_code, r.city
                FROM restaurants r
                JOIN reviews rev ON r.id = rev.restaurant_id
                LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                WHERE rev.created_at > COALESCE(rk.last_computed, '1970-01-01')
                AND r.is_active = 1
            """)
        
        changed_restaurants = [dict_row(r) for r in cur.fetchall()]
        
        if not changed_restaurants:
            cur.close()
            conn.close()
            results['message'] = 'No new reviews found - nothing to update'
            return jsonify(results)
        
        # Group by region
        regions_to_update = {}
        for r in changed_restaurants:
            region = r['region_code']
            if region:
                if region not in regions_to_update:
                    regions_to_update[region] = []
                regions_to_update[region].append(r['id'])
        
        results['restaurants_updated'] = len(changed_restaurants)
        
        cur.close()
        conn.close()
        
        # Step 2: Recompute scores ONLY for changed restaurants, then re-rank regions
        for region_code, restaurant_ids in regions_to_update.items():
            try:
                # Recompute only the changed restaurants
                compute_rankings_selective(region_code, restaurant_ids)
                results['regions_reranked'].append({
                    'region': region_code,
                    'restaurants_updated': len(restaurant_ids)
                })
            except Exception as e:
                results['errors'].append({'region': region_code, 'error': str(e)})
        
        # Step 3: Auto-push regions that have changes AND auto_push enabled
        conn = get_db()
        cur = get_cursor(conn)
        
        cur.execute("SELECT code, auto_push, push_top_n FROM regions WHERE is_active=1 AND auto_push=1")
        auto_push_regions = [dict_row(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        
        for reg in auto_push_regions:
            if reg['code'] in regions_to_update:
                try:
                    # Publish top N - join with restaurants to filter by region
                    conn = get_db()
                    cur = conn.cursor()
                    top_n = reg.get('push_top_n', 20)
                    region = reg['code']
                    
                    # Unpublish all in region via restaurant join
                    if USE_POSTGRES:
                        cur.execute("""UPDATE rankings SET is_published=0 
                            WHERE restaurant_id IN (SELECT id FROM restaurants WHERE canton=%s OR LOWER(canton)=LOWER(%s) OR LOWER(city)=LOWER(%s))""", 
                            (region, region, region))
                        cur.execute("""UPDATE rankings SET is_published=1 WHERE restaurant_id IN (
                            SELECT rk.restaurant_id FROM rankings rk
                            JOIN restaurants r ON rk.restaurant_id = r.id
                            WHERE r.canton=%s OR LOWER(r.canton)=LOWER(%s) OR LOWER(r.city)=LOWER(%s)
                            ORDER BY COALESCE(rk.manual_rank, rk.auto_rank) LIMIT %s)""", 
                            (region, region, region, top_n))
                    else:
                        cur.execute("""UPDATE rankings SET is_published=0 
                            WHERE restaurant_id IN (SELECT id FROM restaurants WHERE canton=? OR LOWER(canton)=LOWER(?) OR LOWER(city)=LOWER(?))""", 
                            (region, region, region))
                        cur.execute("""UPDATE rankings SET is_published=1 WHERE restaurant_id IN (
                            SELECT rk.restaurant_id FROM rankings rk
                            JOIN restaurants r ON rk.restaurant_id = r.id
                            WHERE r.canton=? OR LOWER(r.canton)=LOWER(?) OR LOWER(r.city)=LOWER(?)
                            ORDER BY COALESCE(rk.manual_rank, rk.auto_rank) LIMIT ?)""", 
                            (region, region, region, top_n))
                    conn.commit()
                    cur.close()
                    conn.close()
                    
                    results['regions_pushed'].append(reg['code'])
                except Exception as e:
                    results['errors'].append({'region': reg['code'], 'error': str(e), 'phase': 'push'})
            else:
                results['skipped_no_changes'].append(reg['code'])
        
        # Log task
        conn = get_db()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("""INSERT INTO daily_tasks (task_type, status, details)
                VALUES ('daily_recompute_optimized', 'completed', %s)""", (json.dumps(results),))
        else:
            cur.execute("""INSERT INTO daily_tasks (task_type, status, details)
                VALUES ('daily_recompute_optimized', 'completed', ?)""", (json.dumps(results),))
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify(results)
    
    @app.route('/api/daily/force', methods=['POST'])
    def force_daily_tasks():
        """Force full recomputation of all regions (use sparingly)"""
        data = request.json or {}
        results = {'regions_computed': [], 'errors': []}
        
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT code FROM regions WHERE is_active=1")
        regions = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        
        for region_code in regions:
            try:
                compute_rankings(region_code)
                results['regions_computed'].append(region_code)
            except Exception as e:
                results['errors'].append({'region': region_code, 'error': str(e)})
        
        return jsonify(results)
    
    @app.route('/api/usage')
    def usage():
        return jsonify({'daily': [], 'monthly': {}})

    # ========== DEMO DATA ==========
    
    @app.route('/api/db/health', methods=['GET'])
    def database_health_check():
        """Comprehensive database health check - returns full schema and data analysis"""
        report = {
            'database_type': 'PostgreSQL' if USE_POSTGRES else 'SQLite',
            'timestamp': datetime.now().isoformat(),
            'tables': {},
            'data_counts': {},
            'schema_analysis': {},
            'compatibility_issues': [],
            'recommendations': [],
            'sample_data': {},
            'foreign_key_check': {},
            'errors': []
        }
        
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # ========== 1. GET ALL TABLES ==========
            if USE_POSTGRES:
                cur.execute("""SELECT table_name FROM information_schema.tables 
                    WHERE table_schema='public' ORDER BY table_name""")
                tables = [row[0] if not isinstance(row, dict) else row['table_name'] for row in cur.fetchall()]
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                tables = [row[0] for row in cur.fetchall()]
            
            report['tables_found'] = tables
            
            # ========== 2. GET SCHEMA FOR EACH TABLE ==========
            for table in tables:
                try:
                    if USE_POSTGRES:
                        cur.execute(f"""
                            SELECT column_name, data_type, is_nullable, column_default,
                                   character_maximum_length
                            FROM information_schema.columns 
                            WHERE table_name=%s 
                            ORDER BY ordinal_position
                        """, (table,))
                        columns = []
                        for row in cur.fetchall():
                            col = dict_row(row) if hasattr(row, 'keys') else {
                                'column_name': row[0],
                                'data_type': row[1],
                                'is_nullable': row[2],
                                'column_default': row[3],
                                'max_length': row[4]
                            }
                            columns.append(col)
                    else:
                        cur.execute(f"PRAGMA table_info({table})")
                        columns = [{
                            'column_name': row[1],
                            'data_type': row[2],
                            'is_nullable': 'YES' if not row[3] else 'NO',
                            'column_default': row[4],
                            'is_pk': row[5]
                        } for row in cur.fetchall()]
                    
                    report['tables'][table] = {
                        'columns': columns,
                        'column_names': [c['column_name'] for c in columns]
                    }
                except Exception as e:
                    report['errors'].append(f"Schema error for {table}: {str(e)}")
            
            # ========== 3. GET ROW COUNTS ==========
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                    row = cur.fetchone()
                    count = dict_row(row).get('cnt', 0) if row else 0
                    report['data_counts'][table] = count
                except Exception as e:
                    report['data_counts'][table] = f"error: {str(e)[:50]}"
            
            # ========== 4. CHECK KEY RELATIONSHIPS ==========
            
            # Check restaurants.canton vs regions.code
            try:
                cur.execute("SELECT DISTINCT canton FROM restaurants WHERE canton IS NOT NULL")
                restaurant_cantons = [dict_row(r).get('canton') for r in cur.fetchall()]
                
                cur.execute("SELECT DISTINCT code FROM regions")
                region_codes = [dict_row(r).get('code') for r in cur.fetchall()]
                
                report['foreign_key_check']['restaurants_to_regions'] = {
                    'restaurant_cantons': restaurant_cantons,
                    'region_codes': region_codes,
                    'orphaned_cantons': [c for c in restaurant_cantons if c and c not in region_codes],
                    'unused_regions': [c for c in region_codes if c and c not in restaurant_cantons]
                }
            except Exception as e:
                report['errors'].append(f"FK check restaurants->regions: {str(e)}")
            
            # Check rankings.restaurant_id vs restaurants.id
            try:
                if 'rankings' in tables:
                    # Get rankings columns first to know what to query
                    ranking_cols = report['tables'].get('rankings', {}).get('column_names', [])
                    
                    cur.execute("SELECT DISTINCT restaurant_id FROM rankings")
                    ranking_rest_ids = [dict_row(r).get('restaurant_id') for r in cur.fetchall()]
                    
                    cur.execute("SELECT DISTINCT id FROM restaurants")
                    restaurant_ids = [dict_row(r).get('id') for r in cur.fetchall()]
                    
                    report['foreign_key_check']['rankings_to_restaurants'] = {
                        'rankings_restaurant_ids_count': len(ranking_rest_ids),
                        'restaurants_ids_count': len(restaurant_ids),
                        'orphaned_rankings': len([r for r in ranking_rest_ids if r and r not in restaurant_ids])
                    }
                    
                    # Check region linkage in rankings
                    if 'region_code' in ranking_cols:
                        cur.execute("SELECT DISTINCT region_code FROM rankings WHERE region_code IS NOT NULL")
                        ranking_regions = [dict_row(r).get('region_code') for r in cur.fetchall()]
                        report['foreign_key_check']['rankings_regions'] = {
                            'column_used': 'region_code',
                            'values': ranking_regions
                        }
                    elif 'canton' in ranking_cols:
                        cur.execute("SELECT DISTINCT canton FROM rankings WHERE canton IS NOT NULL")
                        ranking_cantons = [dict_row(r).get('canton') for r in cur.fetchall()]
                        report['foreign_key_check']['rankings_regions'] = {
                            'column_used': 'canton',
                            'values': ranking_cantons
                        }
                    else:
                        report['foreign_key_check']['rankings_regions'] = {
                            'column_used': 'NONE FOUND',
                            'note': 'Rankings table has no region_code or canton column'
                        }
            except Exception as e:
                report['errors'].append(f"FK check rankings->restaurants: {str(e)}")
            
            # ========== 5. SAMPLE DATA FROM KEY TABLES ==========
            sample_tables = ['restaurants', 'regions', 'rankings', 'source_ratings']
            for table in sample_tables:
                if table in tables:
                    try:
                        cur.execute(f"SELECT * FROM {table} LIMIT 3")
                        rows = [dict_row(r) for r in cur.fetchall()]
                        # Convert any non-serializable values
                        for row in rows:
                            for k, v in row.items():
                                row[k] = dt_to_str(v)
                        report['sample_data'][table] = rows
                    except Exception as e:
                        report['sample_data'][table] = f"error: {str(e)[:100]}"
            
            # ========== 6. SCHEMA COMPATIBILITY ANALYSIS ==========
            
            # Expected columns for each table (what my code expects)
            expected_schema = {
                'restaurants': ['id', 'name', 'address', 'city', 'canton', 'country', 'cuisine_type', 
                               'price_range', 'latitude', 'longitude', 'phone', 'website', 'is_active'],
                'regions': ['id', 'code', 'name', 'country', 'latitude', 'longitude', 'restaurants_count'],
                'rankings': ['id', 'restaurant_id', 'region_code', 'composite_score', 'auto_rank', 
                            'manual_rank', 'is_published', 'is_pushed', 'is_featured', 'total_reviews',
                            'sentiment_avg', 'confidence_score', 'data_quality_avg', 'user_reviews_count'],
                'source_ratings': ['id', 'restaurant_id', 'source', 'avg_rating', 'review_count'],
                'reviews': ['id', 'restaurant_id', 'source', 'rating', 'review_text', 'sentiment_score']
            }
            
            for table, expected_cols in expected_schema.items():
                if table in report['tables']:
                    actual_cols = report['tables'][table]['column_names']
                    missing = [c for c in expected_cols if c not in actual_cols]
                    extra = [c for c in actual_cols if c not in expected_cols]
                    
                    report['schema_analysis'][table] = {
                        'expected_columns': expected_cols,
                        'actual_columns': actual_cols,
                        'missing_columns': missing,
                        'extra_columns': extra,
                        'compatible': len(missing) == 0
                    }
                    
                    if missing:
                        report['compatibility_issues'].append(
                            f"Table '{table}' missing columns: {', '.join(missing)}"
                        )
            
            # ========== 7. GENERATE RECOMMENDATIONS ==========
            
            # Check rankings region column
            if 'rankings' in report['tables']:
                ranking_cols = report['tables']['rankings']['column_names']
                if 'region_code' not in ranking_cols:
                    if 'canton' in ranking_cols:
                        report['recommendations'].append(
                            "Rankings table uses 'canton' instead of 'region_code'. Code needs to use 'canton' for region queries."
                        )
                    else:
                        report['recommendations'].append(
                            "Rankings table has no region column. Need to add 'region_code' or 'canton' column."
                        )
            
            # Check data presence
            if report['data_counts'].get('restaurants', 0) > 0 and report['data_counts'].get('rankings', 0) == 0:
                report['recommendations'].append(
                    "Restaurants exist but rankings are empty. Need to run compute_rankings() for each region."
                )
            
            if report['data_counts'].get('restaurants', 0) > 0 and report['data_counts'].get('source_ratings', 0) == 0:
                report['recommendations'].append(
                    "Restaurants exist but source_ratings are empty. Rankings computation will produce zero scores."
                )
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify(report)
            
        except Exception as e:
            import traceback
            report['errors'].append(f"Main error: {str(e)}")
            report['traceback'] = traceback.format_exc()
            return jsonify(report), 500

    @app.route('/api/demo/diagnose', methods=['GET'])
    def diagnose_db():
        """Diagnose database schema and test insert"""
        results = {
            'database_type': 'PostgreSQL' if USE_POSTGRES else 'SQLite',
            'tables': {},
            'test_insert': None,
            'errors': []
        }
        
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Get all tables
            if USE_POSTGRES:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                tables = [row['table_name'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
            else:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]
            
            results['tables_found'] = tables
            
            # Get restaurants table schema
            if 'restaurants' in tables:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name, data_type, is_nullable 
                        FROM information_schema.columns WHERE table_name='restaurants' ORDER BY ordinal_position""")
                    columns = [{'name': row['column_name'], 'type': row['data_type'], 'nullable': row['is_nullable']} 
                               if isinstance(row, dict) else {'name': row[0], 'type': row[1], 'nullable': row[2]}
                               for row in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(restaurants)")
                    columns = [{'name': row[1], 'type': row[2], 'nullable': 'YES' if not row[3] else 'NO'} for row in cur.fetchall()]
                
                results['restaurants_columns'] = columns
                results['column_names'] = [c['name'] for c in columns]
            
            # Get regions table schema
            if 'regions' in tables:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='regions'""")
                    results['regions_columns'] = [row['column_name'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(regions)")
                    results['regions_columns'] = [row[1] for row in cur.fetchall()]
            
            # Get source_ratings schema
            if 'source_ratings' in tables:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='source_ratings'""")
                    results['source_ratings_columns'] = [row['column_name'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(source_ratings)")
                    results['source_ratings_columns'] = [row[1] for row in cur.fetchall()]
            
            # Get scrape_jobs schema
            if 'scrape_jobs' in tables:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='scrape_jobs'""")
                    results['scrape_jobs_columns'] = [row['column_name'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(scrape_jobs)")
                    results['scrape_jobs_columns'] = [row[1] for row in cur.fetchall()]
            
            # Get rankings schema - CRITICAL for fixing queries
            if 'rankings' in tables:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='rankings' ORDER BY ordinal_position""")
                    results['rankings_columns'] = [row['column_name'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(rankings)")
                    results['rankings_columns'] = [row[1] for row in cur.fetchall()]
            
            # Check if email_config exists
            results['email_config_exists'] = 'email_config' in tables
            results['email_queue_exists'] = 'email_queue' in tables
            
            # Check existing data counts
            try:
                cur.execute("SELECT COUNT(*) FROM regions")
                row = cur.fetchone()
                results['regions_count'] = row['count'] if isinstance(row, dict) else row[0]
            except Exception as e:
                results['errors'].append(f"Count regions: {str(e)}")
            
            try:
                cur.execute("SELECT COUNT(*) FROM restaurants")
                row = cur.fetchone()
                results['restaurants_count'] = row['count'] if isinstance(row, dict) else row[0]
            except Exception as e:
                results['errors'].append(f"Count restaurants: {str(e)}")
            
            # Try a test insert into regions
            try:
                if USE_POSTGRES:
                    cur.execute("INSERT INTO regions (code, name, latitude, longitude) VALUES (%s, %s, %s, %s) ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name RETURNING code",
                        ('test_region', 'Test Region', 46.0, 6.0))
                    result = cur.fetchone()
                    results['test_region_insert'] = 'SUCCESS' if result else 'NO RETURN'
                else:
                    cur.execute("INSERT OR REPLACE INTO regions (code, name, latitude, longitude) VALUES (?, ?, ?, ?)",
                        ('test_region', 'Test Region', 46.0, 6.0))
                    results['test_region_insert'] = 'SUCCESS'
                conn.commit()
            except Exception as e:
                results['test_region_insert'] = f'FAILED: {str(e)}'
                results['errors'].append(f"Test region insert: {str(e)}")
            
            # Try a test insert into restaurants - use canton instead of region_code
            try:
                if USE_POSTGRES:
                    cur.execute("""INSERT INTO restaurants (name, address, city, canton, country, latitude, longitude, is_active) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                        ('Test Restaurant Demo', 'Test Address 1', 'Test', 'GE', 'Switzerland', 46.0, 6.0, 1))
                    result = cur.fetchone()
                    rest_id = result['id'] if isinstance(result, dict) else result[0] if result else None
                    results['test_restaurant_insert'] = f'SUCCESS - ID: {rest_id}'
                else:
                    cur.execute("""INSERT INTO restaurants (name, address, city, canton, country, latitude, longitude, is_active) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        ('Test Restaurant Demo', 'Test Address 1', 'Test', 'GE', 'Switzerland', 46.0, 6.0, 1))
                    results['test_restaurant_insert'] = f'SUCCESS - ID: {cur.lastrowid}'
                conn.commit()
            except Exception as e:
                results['test_restaurant_insert'] = f'FAILED: {str(e)}'
                results['errors'].append(f"Test restaurant insert: {str(e)}")
                try:
                    conn.rollback()
                except:
                    pass
            
            # Clean up test data
            try:
                if USE_POSTGRES:
                    cur.execute("DELETE FROM restaurants WHERE name=%s", ('Test Restaurant Demo',))
                    cur.execute("DELETE FROM regions WHERE code=%s", ('test_region',))
                else:
                    cur.execute("DELETE FROM restaurants WHERE name=?", ('Test Restaurant Demo',))
                    cur.execute("DELETE FROM regions WHERE code=?", ('test_region',))
                conn.commit()
                results['cleanup'] = 'SUCCESS'
            except Exception as e:
                results['cleanup'] = f'FAILED: {str(e)}'
            
            cur.close()
            conn.close()
            
        except Exception as e:
            import traceback
            results['errors'].append(f"Main error: {str(e)}")
            results['traceback'] = traceback.format_exc()[:500]
        
        return jsonify(results)
    
    @app.route('/api/debug/data')
    def debug_data():
        """Show exactly what's in the database"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            results = {}
            
            # First, get rankings table columns
            try:
                if USE_POSTGRES:
                    cur.execute("""SELECT column_name FROM information_schema.columns 
                        WHERE table_name='rankings' ORDER BY ordinal_position""")
                    results['rankings_columns'] = [r[0] for r in cur.fetchall()]
                else:
                    cur.execute("PRAGMA table_info(rankings)")
                    results['rankings_columns'] = [r[1] for r in cur.fetchall()]
            except Exception as e:
                results['rankings_columns_error'] = str(e)
            
            # Count restaurants
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM restaurants")
                r = cur.fetchone()
                row = dict_row(r) if r else {}
                results['restaurants_total'] = row.get('cnt', 0)
            except:
                results['restaurants_total'] = 'error'
            
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM restaurants WHERE is_active=1")
                r = cur.fetchone()
                row = dict_row(r) if r else {}
                results['restaurants_active'] = row.get('cnt', 0)
            except:
                results['restaurants_active'] = 'error'
            
            # Sample restaurants
            try:
                cur.execute("SELECT id, name, city, canton, country, is_active FROM restaurants LIMIT 10")
                results['sample_restaurants'] = [dict_row(r) for r in cur.fetchall()]
            except Exception as e:
                results['sample_restaurants_error'] = str(e)
            
            # Count regions
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM regions")
                r = cur.fetchone()
                row = dict_row(r) if r else {}
                results['regions_total'] = row.get('cnt', 0)
            except:
                results['regions_total'] = 'error'
            
            # Sample regions
            try:
                cur.execute("SELECT code, name, restaurants_count FROM regions LIMIT 10")
                results['sample_regions'] = [dict_row(r) for r in cur.fetchall()]
            except Exception as e:
                results['sample_regions_error'] = str(e)
            
            # Count rankings
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM rankings")
                r = cur.fetchone()
                row = dict_row(r) if r else {}
                results['rankings_total'] = row.get('cnt', 0)
            except:
                results['rankings_total'] = 'error'
            
            # Sample rankings - use only columns we know exist
            try:
                cur.execute("SELECT * FROM rankings LIMIT 5")
                results['sample_rankings'] = [dict_row(r) for r in cur.fetchall()]
            except Exception as e:
                results['sample_rankings_error'] = str(e)
            
            # Count source ratings
            try:
                cur.execute("SELECT COUNT(*) as cnt FROM source_ratings")
                r = cur.fetchone()
                row = dict_row(r) if r else {}
                results['source_ratings_total'] = row.get('cnt', 0)
            except:
                results['source_ratings_total'] = 'error'
            
            # Check canton values in restaurants
            try:
                cur.execute("SELECT canton, COUNT(*) as cnt FROM restaurants GROUP BY canton ORDER BY cnt DESC")
                results['restaurants_by_canton'] = [dict_row(r) for r in cur.fetchall()]
            except Exception as e:
                results['restaurants_by_canton_error'] = str(e)
            
            # Check region codes in regions table
            try:
                cur.execute("SELECT code, name FROM regions ORDER BY code")
                results['region_codes'] = [dict_row(r) for r in cur.fetchall()]
            except Exception as e:
                results['region_codes_error'] = str(e)
            
            conn.commit()  # Clear any transaction state
            cur.close()
            conn.close()
            
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({'error': str(e), 'trace': traceback.format_exc()[:500]}), 500
    
    @app.route('/api/demo/load', methods=['POST'])
    def load_demo_data():
        """Load comprehensive demo data to test the full system"""
        errors = []
        
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Demo regions - using canton codes that match Swiss cantons
            demo_regions = [
                ('GE', 'Geneva', 'Switzerland', 46.2044, 6.1432),
                ('ZH', 'Zurich', 'Switzerland', 47.3769, 8.5417),
                ('VD', 'Lausanne', 'Switzerland', 46.5197, 6.6323),
                ('BS', 'Basel', 'Switzerland', 47.5596, 7.5886),
                ('BE', 'Bern', 'Switzerland', 46.9480, 7.4474),
            ]
            
            regions_created = 0
            for code, name, country, lat, lng in demo_regions:
                try:
                    if USE_POSTGRES:
                        # Check if exists first
                        cur.execute("SELECT code FROM regions WHERE code=%s", (code,))
                        if cur.fetchone():
                            cur.execute("UPDATE regions SET name=%s, latitude=%s, longitude=%s WHERE code=%s", (name, lat, lng, code))
                        else:
                            cur.execute("INSERT INTO regions (code, name, country, latitude, longitude) VALUES (%s, %s, %s, %s, %s)", 
                                (code, name, country, lat, lng))
                    else:
                        cur.execute("INSERT OR REPLACE INTO regions (code, name, country, latitude, longitude) VALUES (?, ?, ?, ?, ?)", 
                            (code, name, country, lat, lng))
                    conn.commit()  # Commit each region separately
                    regions_created += 1
                except Exception as e:
                    errors.append(f"Region {code}: {str(e)[:50]}")
                    try:
                        conn.rollback()
                    except:
                        pass
            
            # Demo restaurants - using canton field instead of region_code
            demo_restaurants = [
                # Geneva (GE)
                ("Le Chat-Botté", "GE", "Geneva", "Quai du Général-Guisan 2", "French Gastronomic", "$$$$", 4.8),
                ("Bayview Restaurant", "GE", "Geneva", "Quai du Mont-Blanc 13", "Contemporary", "$$$$", 4.7),
                ("Café du Soleil Genève", "GE", "Geneva", "Place du Petit-Saconnex 6", "Swiss Traditional", "$$", 4.4),
                ("Brasserie Lipp Genève", "GE", "Geneva", "Rue de la Confédération 8", "French Brasserie", "$$$", 4.2),
                ("Roberto Ristorante", "GE", "Geneva", "Rue Pierre-Fatio 10", "Italian", "$$$", 4.3),
                ("La Bottega Genève", "GE", "Geneva", "Rue de la Fontaine 22", "Italian Casual", "$$", 4.1),
                ("Miyako Japanese GE", "GE", "Geneva", "Rue de Chantepoulet 11", "Japanese", "$$$", 4.4),
                ("Izumi Restaurant", "GE", "Geneva", "Quai des Bergues 33", "Japanese Fusion", "$$$$", 4.5),
                ("Thai Phuket Genève", "GE", "Geneva", "Rue de Berne 8", "Thai", "$$", 4.2),
                ("L'Entrecôte Couronnée", "GE", "Geneva", "Rue du Rhône 5", "French Steakhouse", "$$$", 4.3),
                # Zurich (ZH)
                ("The Dolder Grand", "ZH", "Zurich", "Kurhausstrasse 65", "Fine Dining", "$$$$", 4.9),
                ("Kronenhalle Zürich", "ZH", "Zurich", "Rämistrasse 4", "Swiss Classic", "$$$$", 4.7),
                ("Clouds Restaurant ZH", "ZH", "Zurich", "Maagplatz 5", "Modern European", "$$$$", 4.6),
                ("Haus Hiltl", "ZH", "Zurich", "Sihlstrasse 28", "Vegetarian", "$$$", 4.5),
                ("Zeughauskeller Zürich", "ZH", "Zurich", "Bahnhofstrasse 28a", "Swiss Traditional", "$$", 4.3),
                ("Café Sprüngli", "ZH", "Zurich", "Bahnhofstrasse 21", "Swiss Café", "$$$", 4.4),
                ("Sala of Tokyo ZH", "ZH", "Zurich", "Limmatstrasse 29", "Japanese", "$$$", 4.5),
                ("Bianchi Ristorante", "ZH", "Zurich", "Limmatquai 82", "Italian", "$$$", 4.3),
                # Lausanne (VD)
                ("Anne-Sophie Pic", "VD", "Lausanne", "Avenue de Cour 4", "French Gastronomic", "$$$$", 4.9),
                ("Le Berceau des Sens", "VD", "Lausanne", "Route de Cojonnex 18", "Modern French", "$$$$", 4.7),
                ("Café de Grancy", "VD", "Lausanne", "Avenue du Rond-Point 1", "Swiss Bistro", "$$", 4.3),
                ("Le Barbare Lausanne", "VD", "Lausanne", "Escaliers du Marché 27", "Fondue", "$$", 4.4),
                ("Pinte Besson", "VD", "Lausanne", "Rue de l'Ale 4", "Swiss Traditional", "$$", 4.2),
                # Basel (BS)
                ("Restaurant Stucki", "BS", "Basel", "Bruderholzallee 42", "French Gastronomic", "$$$$", 4.8),
                ("Cheval Blanc Basel", "BS", "Basel", "Blumenrain 8", "Fine Dining", "$$$$", 4.9),
                ("Kunsthalle Basel", "BS", "Basel", "Steinenberg 7", "Modern European", "$$$", 4.4),
                ("Zum Goldenen Sternen", "BS", "Basel", "St. Alban-Rheinweg 70", "Swiss", "$$$", 4.3),
                # Bern (BE)
                ("Meridiano Bern", "BE", "Bern", "Kochergasse 3", "Modern Swiss", "$$$$", 4.7),
                ("Kornhauskeller Bern", "BE", "Bern", "Kornhausplatz 18", "Swiss Classic", "$$$", 4.5),
                ("Restaurant Rosengarten", "BE", "Bern", "Alter Aargauerstalden 31b", "Swiss", "$$", 4.4),
                ("Altes Tramdepot", "BE", "Bern", "Am Bärengraben 14", "Brewery", "$$", 4.3),
            ]
            
            total_restaurants = 0
            total_reviews = 0
            
            base_coords = {'GE': (46.2044, 6.1432), 'ZH': (47.3769, 8.5417), 'VD': (46.5197, 6.6323), 'BS': (47.5596, 7.5886), 'BE': (46.9480, 7.4474)}
            
            for name, canton, city, address, cuisine, price_range, base_rating in demo_restaurants:
                try:
                    base_lat, base_lng = base_coords.get(canton, (46.8, 8.2))
                    lat = round(base_lat + random.uniform(-0.02, 0.02), 6)
                    lng = round(base_lng + random.uniform(-0.02, 0.02), 6)
                    phone = f"+41 {random.randint(21, 79)} {random.randint(100, 999)} {random.randint(10, 99)} {random.randint(10, 99)}"
                    
                    # Check if exists first
                    if USE_POSTGRES:
                        cur.execute("SELECT id FROM restaurants WHERE name=%s AND city=%s LIMIT 1", (name, city))
                    else:
                        cur.execute("SELECT id FROM restaurants WHERE name=? AND city=? LIMIT 1", (name, city))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        rest_id = existing['id'] if isinstance(existing, dict) else existing[0]
                    else:
                        # INSERT using correct schema
                        if USE_POSTGRES:
                            cur.execute("""INSERT INTO restaurants (name, address, city, canton, country, cuisine_type, price_range, latitude, longitude, phone, is_active)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                                (name, address, city, canton, 'Switzerland', cuisine, price_range, lat, lng, phone, 1))
                            result = cur.fetchone()
                            rest_id = result['id'] if isinstance(result, dict) else result[0] if result else None
                        else:
                            cur.execute("""INSERT INTO restaurants (name, address, city, canton, country, cuisine_type, price_range, latitude, longitude, phone, is_active)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                (name, address, city, canton, 'Switzerland', cuisine, price_range, lat, lng, phone, 1))
                            rest_id = cur.lastrowid
                    
                    conn.commit()  # Commit each restaurant separately
                    
                    if not rest_id:
                        errors.append(f"No ID for {name}")
                        continue
                    
                    total_restaurants += 1
                    
                    # Add source ratings
                    for source in ['google', 'yelp', 'tripadvisor']:
                        try:
                            avg_rating = round(base_rating + random.uniform(-0.3, 0.2), 2)
                            review_count = random.randint(50, 500)
                            
                            # Check if exists first
                            if USE_POSTGRES:
                                cur.execute("SELECT id FROM source_ratings WHERE restaurant_id=%s AND source=%s LIMIT 1", (rest_id, source))
                            else:
                                cur.execute("SELECT id FROM source_ratings WHERE restaurant_id=? AND source=? LIMIT 1", (rest_id, source))
                            
                            existing_rating = cur.fetchone()
                            
                            if existing_rating:
                                existing_id = existing_rating['id'] if isinstance(existing_rating, dict) else existing_rating[0]
                                if USE_POSTGRES:
                                    cur.execute("UPDATE source_ratings SET avg_rating=%s, review_count=%s, last_updated=NOW() WHERE id=%s",
                                        (avg_rating, review_count, existing_id))
                                else:
                                    cur.execute("UPDATE source_ratings SET avg_rating=?, review_count=?, last_updated=CURRENT_TIMESTAMP WHERE id=?",
                                        (avg_rating, review_count, existing_id))
                            else:
                                if USE_POSTGRES:
                                    cur.execute("""INSERT INTO source_ratings (restaurant_id, source, avg_rating, review_count)
                                        VALUES (%s, %s, %s, %s)""", (rest_id, source, avg_rating, review_count))
                                else:
                                    cur.execute("""INSERT INTO source_ratings (restaurant_id, source, avg_rating, review_count)
                                        VALUES (?, ?, ?, ?)""", (rest_id, source, avg_rating, review_count))
                            
                            conn.commit()  # Commit each rating
                            total_reviews += review_count
                        except Exception as e:
                            errors.append(f"Rating {name}/{source}: {str(e)[:30]}")
                            try:
                                conn.rollback()
                            except:
                                pass
                            
                except Exception as e:
                    errors.append(f"{name}: {str(e)[:50]}")
                    try:
                        conn.rollback()
                    except:
                        pass
                    continue
            
            # Update region stats
            for code, name, country, lat, lng in demo_regions:
                try:
                    if USE_POSTGRES:
                        cur.execute("UPDATE regions SET last_scan=NOW(), restaurants_count=(SELECT COUNT(*) FROM restaurants WHERE canton=%s) WHERE code=%s", (code, code))
                    else:
                        cur.execute("UPDATE regions SET last_scan=CURRENT_TIMESTAMP, restaurants_count=(SELECT COUNT(*) FROM restaurants WHERE canton=?) WHERE code=?", (code, code))
                    conn.commit()
                except Exception as e:
                    try:
                        conn.rollback()
                    except:
                        pass
            
            # Init ranking weights
            try:
                cur.execute("SELECT id FROM ranking_weights WHERE is_active=1")
                if not cur.fetchone():
                    if USE_POSTGRES:
                        cur.execute("INSERT INTO ranking_weights (name, is_active) VALUES ('default', 1)")
                    else:
                        cur.execute("INSERT INTO ranking_weights (name, is_active) VALUES ('default', 1)")
                conn.commit()
            except:
                try:
                    conn.rollback()
                except:
                    pass
            
            cur.close()
            conn.close()
            
            # Compute rankings for all regions
            for code, _, _, _, _ in demo_regions:
                try:
                    compute_rankings(code)
                except Exception as e:
                    errors.append(f"Ranking {code}: {str(e)[:30]}")
            
            result = {
                'success': True,
                'regions_created': regions_created,
                'restaurants_created': total_restaurants,
                'reviews_created': total_reviews,
                'message': f'Demo data loaded: {total_restaurants} restaurants across {regions_created} regions'
            }
            if errors:
                result['warnings'] = errors[:10]
            return jsonify(result)
            
        except Exception as e:
            import traceback
            return jsonify({'error': str(e), 'trace': traceback.format_exc()[:500]}), 500
    
    @app.route('/api/demo/clear', methods=['POST'])
    def clear_demo_data():
        """Clear all data from the database"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Clear in order due to foreign keys - with individual commits
            tables = ['email_queue', 'email_log', 'push_history', 'daily_tasks', 'rankings', 'source_ratings', 'reviews', 'restaurants', 'scrape_jobs', 'regions']
            
            for table in tables:
                try:
                    cur.execute(f"DELETE FROM {table}")
                    conn.commit()
                except Exception as e:
                    try:
                        conn.rollback()
                    except:
                        pass
            
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'message': 'All data cleared'})
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ========== BACKUP / EXPORT ==========
    
    @app.route('/api/backup', methods=['GET'])
    def backup_data():
        """Export all rankings data as JSON for backup"""
        try:
            region = request.args.get('region')
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            
            # Get regions
            if region:
                cur.execute(f"SELECT * FROM regions WHERE code={p}", (region,))
            else:
                cur.execute("SELECT * FROM regions ORDER BY name")
            regions_data = [dict_row(r) for r in cur.fetchall()]
            
            # Get restaurants with rankings - only use columns that exist
            if region:
                if USE_POSTGRES:
                    cur.execute("""SELECT r.*, rk.composite_score, rk.sentiment_avg, rk.confidence_score,
                        rk.total_reviews, rk.auto_rank, rk.manual_rank, rk.is_featured, rk.is_published,
                        rk.admin_notes, rk.last_computed
                        FROM restaurants r
                        LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                        WHERE r.canton = %s OR LOWER(r.canton) = LOWER(%s) OR LOWER(r.city) = LOWER(%s)
                        ORDER BY COALESCE(rk.manual_rank, rk.auto_rank, 999)""", (region, region, region))
                else:
                    cur.execute("""SELECT r.*, rk.composite_score, rk.sentiment_avg, rk.confidence_score,
                        rk.total_reviews, rk.auto_rank, rk.manual_rank, rk.is_featured, rk.is_published,
                        rk.admin_notes, rk.last_computed
                        FROM restaurants r
                        LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                        WHERE r.canton = ? OR LOWER(r.canton) = LOWER(?) OR LOWER(r.city) = LOWER(?)
                        ORDER BY COALESCE(rk.manual_rank, rk.auto_rank, 999)""", (region, region, region))
            else:
                cur.execute("""SELECT r.*, rk.composite_score, rk.sentiment_avg, rk.confidence_score,
                    rk.total_reviews, rk.auto_rank, rk.manual_rank, rk.is_featured, rk.is_published,
                    rk.admin_notes, rk.last_computed
                    FROM restaurants r
                    LEFT JOIN rankings rk ON r.id = rk.restaurant_id
                    ORDER BY r.canton, COALESCE(rk.manual_rank, rk.auto_rank, 999)""")
            
            restaurants_data = [dict_row(r) for r in cur.fetchall()]
            
            # Get source ratings
            restaurant_ids = [r['id'] for r in restaurants_data if r]
            source_ratings_data = []
            if restaurant_ids:
                placeholders = ','.join([p] * len(restaurant_ids))
                cur.execute(f"SELECT * FROM source_ratings WHERE restaurant_id IN ({placeholders})", tuple(restaurant_ids))
                source_ratings_data = [dict_row(r) for r in cur.fetchall()]
            
            # Get ranking weights
            cur.execute("SELECT * FROM ranking_weights WHERE is_active=1")
            weights_data = dict_row(cur.fetchone())
            
            cur.close()
            conn.close()
            
            # Convert datetimes to strings
            def serialize(data):
                if isinstance(data, list):
                    return [serialize(item) for item in data]
                if isinstance(data, dict):
                    return {k: dt_to_str(v) for k, v in data.items()}
                return data
            
            backup = {
                'backup_date': datetime.now().isoformat(),
                'backup_type': 'full' if not region else f'region:{region}',
                'regions': serialize(regions_data),
                'restaurants': serialize(restaurants_data),
                'source_ratings': serialize(source_ratings_data),
                'ranking_weights': serialize(weights_data),
                'stats': {
                    'total_regions': len(regions_data),
                    'total_restaurants': len(restaurants_data),
                    'published': len([r for r in restaurants_data if r and r.get('is_published')]),
                    'pushed': len([r for r in restaurants_data if r and r.get('is_pushed')])
                }
            }
            
            return jsonify(backup)
            
        except Exception as e:
            app.logger.error(f'Backup error: {str(e)}')
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/backup/restore', methods=['POST'])
    def restore_backup():
        """Restore rankings from a backup file (manual ranks and notes only)"""
        try:
            data = request.json
            if not data or 'restaurants' not in data:
                return jsonify({'error': 'Invalid backup file'}), 400
            
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            
            restored = 0
            for rest in data.get('restaurants', []):
                if rest.get('id') and (rest.get('manual_rank') or rest.get('admin_notes')):
                    if USE_POSTGRES:
                        cur.execute("""UPDATE rankings SET 
                            manual_rank = COALESCE(%s, manual_rank),
                            admin_notes = COALESCE(%s, admin_notes)
                            WHERE restaurant_id = %s""",
                            (rest.get('manual_rank'), rest.get('admin_notes'), rest['id']))
                    else:
                        cur.execute("""UPDATE rankings SET 
                            manual_rank = COALESCE(?, manual_rank),
                            admin_notes = COALESCE(?, admin_notes)
                            WHERE restaurant_id = ?""",
                            (rest.get('manual_rank'), rest.get('admin_notes'), rest['id']))
                    restored += 1
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'restored': restored})
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ========== EMAIL CONFIGURATION ==========
    
    @app.route('/api/email/config', methods=['GET'])
    def get_email_config():
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Create table if it doesn't exist
            try:
                if USE_POSTGRES:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id SERIAL PRIMARY KEY,
                        smtp_host VARCHAR(255),
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user VARCHAR(255),
                        smtp_password VARCHAR(255),
                        from_name VARCHAR(255) DEFAULT 'Spotwego',
                        reply_to VARCHAR(255),
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )""")
                else:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        smtp_host TEXT,
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user TEXT,
                        smtp_password TEXT,
                        from_name TEXT DEFAULT 'Spotwego',
                        reply_to TEXT,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                conn.commit()
            except:
                pass
            
            cur.execute("SELECT * FROM email_config LIMIT 1")
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                cfg = dict_row(row)
                cfg.pop('smtp_password_encrypted', None)  # Don't send password
                return jsonify(cfg)
            
            return jsonify({
                'smtp_host': 'smtp.gmail.com',
                'smtp_port': 587,
                'smtp_user': '',
                'from_email': '',
                'from_name': 'Spotwego',
                'reply_to': '',
                'is_active': 0
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/config', methods=['POST'])
    def save_email_config():
        try:
            data = request.json
            smtp_password = data.get('smtp_password', '')
            
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            
            # Create table if it doesn't exist
            try:
                if USE_POSTGRES:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id SERIAL PRIMARY KEY,
                        smtp_host VARCHAR(255),
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user VARCHAR(255),
                        smtp_password VARCHAR(255),
                        from_name VARCHAR(255) DEFAULT 'Spotwego',
                        reply_to VARCHAR(255),
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )""")
                else:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        smtp_host TEXT,
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user TEXT,
                        smtp_password TEXT,
                        from_name TEXT DEFAULT 'Spotwego',
                        reply_to TEXT,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                conn.commit()
            except:
                pass
            
            cur.execute("SELECT id FROM email_config LIMIT 1")
            exists = cur.fetchone()
            
            if USE_POSTGRES:
                if exists:
                    exists_id = exists['id'] if isinstance(exists, dict) else exists[0]
                    cur.execute("""UPDATE email_config SET
                        smtp_host=%s, smtp_port=%s, smtp_user=%s, smtp_password=%s,
                        from_name=%s, reply_to=%s, is_active=%s, updated_at=NOW()
                        WHERE id=%s""",
                        (data.get('smtp_host', 'smtp.gmail.com'), data.get('smtp_port', 587),
                         data.get('smtp_user'), smtp_password,
                         data.get('from_name', 'Spotwego'), data.get('reply_to'),
                         data.get('is_active', 1), exists_id))
                else:
                    cur.execute("""INSERT INTO email_config 
                        (smtp_host, smtp_port, smtp_user, smtp_password, from_name, reply_to, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (data.get('smtp_host', 'smtp.gmail.com'), data.get('smtp_port', 587),
                         data.get('smtp_user'), smtp_password,
                         data.get('from_name', 'Spotwego'), data.get('reply_to'),
                         data.get('is_active', 1)))
            else:
                exists_id = exists[0] if exists else None
                if exists_id:
                    cur.execute("""UPDATE email_config SET
                        smtp_host=?, smtp_port=?, smtp_user=?, smtp_password=?,
                        from_name=?, reply_to=?, is_active=?, updated_at=CURRENT_TIMESTAMP
                        WHERE id=?""",
                        (data.get('smtp_host', 'smtp.gmail.com'), data.get('smtp_port', 587),
                         data.get('smtp_user'), smtp_password,
                         data.get('from_name', 'Spotwego'), data.get('reply_to'),
                         data.get('is_active', 1), exists_id))
                else:
                    cur.execute("""INSERT INTO email_config 
                        (smtp_host, smtp_port, smtp_user, smtp_password, from_name, reply_to, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (data.get('smtp_host', 'smtp.gmail.com'), data.get('smtp_port', 587),
                         data.get('smtp_user'), smtp_password,
                         data.get('from_name', 'Spotwego'), data.get('reply_to'),
                         data.get('is_active', 1)))
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/test', methods=['POST'])
    def send_test_email():
        """Send a test email to verify SMTP configuration"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            data = request.json
            recipient = data.get('recipient')
            
            if not recipient:
                return jsonify({'error': 'Recipient email is required'}), 400
            
            # Get email config - create table if it doesn't exist
            conn = get_db()
            cur = get_cursor(conn)
            
            # Try to create email_config table if it doesn't exist
            try:
                if USE_POSTGRES:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id SERIAL PRIMARY KEY,
                        smtp_host VARCHAR(255),
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user VARCHAR(255),
                        smtp_password VARCHAR(255),
                        from_name VARCHAR(255) DEFAULT 'Spotwego',
                        reply_to VARCHAR(255),
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )""")
                else:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_config (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        smtp_host TEXT,
                        smtp_port INTEGER DEFAULT 587,
                        smtp_user TEXT,
                        smtp_password TEXT,
                        from_name TEXT DEFAULT 'Spotwego',
                        reply_to TEXT,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                conn.commit()
            except Exception as e:
                app.logger.warning(f"Could not create email_config table: {e}")
            
            try:
                cur.execute("SELECT * FROM email_config WHERE is_active=1 LIMIT 1")
                row = cur.fetchone()
            except:
                row = None
            
            cur.close()
            conn.close()
            
            if not row:
                return jsonify({'error': 'Email configuration not found. Please save your SMTP settings first.'}), 400
            
            config = dict_row(row)
            
            if not config.get('smtp_host') or not config.get('smtp_user') or not config.get('smtp_password'):
                return jsonify({'error': 'Incomplete SMTP configuration. Please fill in all fields and save.'}), 400
            
            # Create test email
            msg = MIMEMultipart('alternative')
            msg['Subject'] = '🧪 Spotwego Test Email - Configuration Working!'
            msg['From'] = f"{config.get('from_name', 'Spotwego')} <{config.get('smtp_user')}>"
            msg['To'] = recipient
            
            # Plain text version
            text_content = """
Spotwego Test Email

Congratulations! Your email configuration is working correctly.

Configuration Details:
- SMTP Host: {smtp_host}
- SMTP Port: {smtp_port}
- From Name: {from_name}

This is a test email sent from your Spotwego Admin Dashboard.

Best regards,
The Spotwego Team
            """.format(
                smtp_host=config.get('smtp_host'),
                smtp_port=config.get('smtp_port'),
                from_name=config.get('from_name', 'Spotwego')
            )
            
            # HTML version with Spotwego branding
            html_content = """
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
    <div style="background: #6B4444; padding: 20px; text-align: center;">
        <h1 style="color: white; margin: 0; font-family: Georgia, serif; font-style: italic;">Spotwego</h1>
    </div>
    <div style="padding: 30px; background: #FAF7F5;">
        <h2 style="color: #3D2929;">🧪 Test Email Successful!</h2>
        <p style="color: #3D2929;">Congratulations! Your email configuration is <strong style="color: #4CAF50;">working correctly</strong>.</p>
        
        <div style="background: white; border: 1px solid #C4A4A4; border-radius: 8px; padding: 15px; margin: 20px 0;">
            <h4 style="color: #6B4444; margin-top: 0;">Configuration Details:</h4>
            <ul style="color: #3D2929; margin: 0; padding-left: 20px;">
                <li><strong>SMTP Host:</strong> {smtp_host}</li>
                <li><strong>SMTP Port:</strong> {smtp_port}</li>
                <li><strong>From Name:</strong> {from_name}</li>
            </ul>
        </div>
        
        <p style="color: #3D2929;">You can now send welcome emails to restaurants that get featured on your platform.</p>
        <p style="color: #3D2929;">Best regards,<br><strong>The Spotwego Team</strong></p>
    </div>
    <div style="background: #6B4444; padding: 15px; text-align: center; color: #C4A4A4; font-size: 12px;">
        © 2025 Spotwego. All rights reserved.
    </div>
</body>
</html>
            """.format(
                smtp_host=config.get('smtp_host'),
                smtp_port=config.get('smtp_port'),
                from_name=config.get('from_name', 'Spotwego')
            )
            
            msg.attach(MIMEText(text_content, 'plain'))
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            try:
                server = smtplib.SMTP(config['smtp_host'], int(config.get('smtp_port', 587)))
                server.starttls()
                server.login(config['smtp_user'], config['smtp_password'])
                server.sendmail(config['smtp_user'], recipient, msg.as_string())
                server.quit()
                
                return jsonify({
                    'success': True,
                    'message': f'Test email sent to {recipient}'
                })
                
            except smtplib.SMTPAuthenticationError as e:
                return jsonify({'error': f'SMTP Authentication failed. Check your username and password. For Gmail, make sure you are using an App Password. Details: {str(e)}'}), 400
            except smtplib.SMTPConnectError as e:
                return jsonify({'error': f'Could not connect to SMTP server. Check host and port. Details: {str(e)}'}), 400
            except smtplib.SMTPException as e:
                return jsonify({'error': f'SMTP error: {str(e)}'}), 400
                
        except Exception as e:
            import traceback
            app.logger.error(f'Test email error: {traceback.format_exc()}')
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/templates', methods=['GET'])
    def get_email_templates():
        try:
            conn = get_db()
            cur = get_cursor(conn)
            cur.execute("SELECT * FROM email_templates ORDER BY name")
            rows = [dict_row(r) for r in cur.fetchall()]
            cur.close()
            conn.close()
            return jsonify(rows)
        except Exception as e:
            return jsonify([])
    
    @app.route('/api/email/templates/<template_id>', methods=['GET'])
    def get_email_template(template_id):
        """Get a specific email template by name/id"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            cur.execute(f"SELECT * FROM email_templates WHERE name={p} OR id::text={p}", (template_id, template_id))
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if row:
                r = dict_row(row)
                return jsonify({
                    'id': r.get('id'),
                    'name': r.get('name'),
                    'subject': r.get('subject', ''),
                    'body': r.get('body_html', ''),
                    'body_text': r.get('body_text', ''),
                    'variables': json.loads(r.get('variables', '[]')) if r.get('variables') else []
                })
            else:
                # Return default template structure
                return jsonify({
                    'id': template_id,
                    'name': template_id,
                    'subject': '',
                    'body': '',
                    'body_text': '',
                    'variables': ['restaurant_name', 'city', 'rating', 'ranking_url']
                })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/templates', methods=['POST'])
    def save_email_template():
        try:
            data = request.json
            name = data.get('name', data.get('id', 'welcome'))
            subject = data.get('subject', '')
            body_html = data.get('body', data.get('body_html', ''))
            body_text = data.get('body_text', '')
            
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            
            if USE_POSTGRES:
                cur.execute("""INSERT INTO email_templates (name, subject, body_html, body_text, variables)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(name) DO UPDATE SET
                    subject=EXCLUDED.subject, body_html=EXCLUDED.body_html, 
                    body_text=EXCLUDED.body_text, updated_at=NOW()""",
                    (name, subject, body_html, body_text,
                     json.dumps(data.get('variables', ['restaurant_name', 'contact_name', 'city', 'address']))))
            else:
                cur.execute("""INSERT INTO email_templates (name, subject, body_html, body_text, variables)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                    subject=excluded.subject, body_html=excluded.body_html, 
                    body_text=excluded.body_text, updated_at=CURRENT_TIMESTAMP""",
                    (name, subject, body_html, body_text,
                     json.dumps(data.get('variables', ['restaurant_name', 'contact_name', 'city', 'address']))))
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/test-template', methods=['POST'])
    def test_email_template():
        """Send a test email with custom template"""
        try:
            data = request.json
            to_email = data.get('to_email')
            subject = data.get('subject', 'Test Email')
            body = data.get('body', '<p>Test email body</p>')
            
            if not to_email:
                return jsonify({'error': 'to_email required'}), 400
            
            # Replace variables with sample data
            body = body.replace('{{restaurant_name}}', 'Sample Restaurant')
            body = body.replace('{{city}}', 'Geneva')
            body = body.replace('{{rating}}', '4.7')
            body = body.replace('{{ranking_url}}', 'https://spotwego.com/ranking/sample')
            body = body.replace('{{address}}', '123 Sample Street')
            
            # Get SMTP config
            cfg = load_config()
            email_cfg = cfg.get('email', {})
            
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{email_cfg.get('from_name', 'Spotwego')} <{email_cfg.get('from_email', email_cfg.get('username', ''))}>"
            msg['To'] = to_email
            
            msg.attach(MIMEText(body, 'html'))
            
            server = smtplib.SMTP(email_cfg.get('host', 'smtp.gmail.com'), email_cfg.get('port', 587))
            server.starttls()
            server.login(email_cfg.get('username', ''), email_cfg.get('password', ''))
            server.sendmail(msg['From'], [to_email], msg.as_string())
            server.quit()
            
            return jsonify({'success': True, 'message': f'Test email sent to {to_email}'})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/email/log', methods=['GET'])
    def get_email_log():
        try:
            conn = get_db()
            cur = get_cursor(conn)
            cur.execute("""SELECT el.*, r.name as restaurant_name 
                FROM email_log el
                LEFT JOIN restaurants r ON el.restaurant_id = r.id
                ORDER BY el.created_at DESC LIMIT 100""")
            rows = [dict_row(r) for r in cur.fetchall()]
            cur.close()
            conn.close()
            for r in rows:
                if r:
                    for k, v in r.items():
                        r[k] = dt_to_str(v)
            return jsonify(rows)
        except Exception as e:
            return jsonify([])
    
    @app.route('/api/email/send-welcome/<int:restaurant_id>', methods=['POST'])
    def send_welcome(restaurant_id):
        """Manually send welcome email to a restaurant"""
        result = send_welcome_email(restaurant_id)
        if result.get('success'):
            return jsonify(result)
        return jsonify(result), 400
    
    @app.route('/api/email/send-pending', methods=['POST'])
    def send_pending_emails():
        """Legacy endpoint - now redirects to queue generation"""
        return jsonify({'error': 'Please use the email approval queue instead. Go to Email Notifications to approve emails before sending.'}), 400
    
    # ========== EMAIL APPROVAL QUEUE ==========
    
    @app.route('/api/email/queue', methods=['GET'])
    def get_email_queue():
        """Get all emails in the approval queue"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            # Create email_queue table if it doesn't exist
            try:
                if USE_POSTGRES:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_queue (
                        id SERIAL PRIMARY KEY,
                        restaurant_id INTEGER REFERENCES restaurants(id),
                        restaurant_name VARCHAR(255),
                        to_email VARCHAR(255),
                        city VARCHAR(100),
                        status VARCHAR(20) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT NOW(),
                        approved_at TIMESTAMP,
                        sent_at TIMESTAMP
                    )""")
                else:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        restaurant_id INTEGER,
                        restaurant_name TEXT,
                        to_email TEXT,
                        city TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        approved_at TIMESTAMP,
                        sent_at TIMESTAMP
                    )""")
                conn.commit()
            except:
                pass
            
            # Get queue items that are not sent yet
            cur.execute("""SELECT * FROM email_queue 
                WHERE status IN ('pending', 'approved', 'rejected') 
                ORDER BY created_at DESC""")
            rows = [dict_row(r) for r in cur.fetchall()]
            cur.close()
            conn.close()
            return jsonify(rows)
        except Exception as e:
            return jsonify([])
    
    @app.route('/api/email/queue/generate', methods=['POST'])
    def generate_email_queue():
        """Generate queue entries for restaurants that need welcome emails"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            p = '%s' if USE_POSTGRES else '?'
            
            # Create table if not exists
            try:
                if USE_POSTGRES:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_queue (
                        id SERIAL PRIMARY KEY,
                        restaurant_id INTEGER REFERENCES restaurants(id),
                        restaurant_name VARCHAR(255),
                        to_email VARCHAR(255),
                        city VARCHAR(100),
                        status VARCHAR(20) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT NOW(),
                        approved_at TIMESTAMP,
                        sent_at TIMESTAMP
                    )""")
                else:
                    cur.execute("""CREATE TABLE IF NOT EXISTS email_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        restaurant_id INTEGER,
                        restaurant_name TEXT,
                        to_email TEXT,
                        city TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        approved_at TIMESTAMP,
                        sent_at TIMESTAMP
                    )""")
                conn.commit()
            except:
                pass
            
            # Find restaurants that are pushed but not already in queue
            # Try different column names for compatibility
            try:
                cur.execute("""SELECT r.id, r.name, r.city, r.website as email 
                    FROM restaurants r
                    JOIN rankings rk ON r.id = rk.restaurant_id
                    WHERE rk.is_pushed = 1 
                    AND r.id NOT IN (SELECT restaurant_id FROM email_queue WHERE status != 'rejected')""")
            except:
                # Fallback - just get all restaurants that might need emails
                cur.execute("""SELECT r.id, r.name, r.city, r.website as email 
                    FROM restaurants r
                    WHERE r.id NOT IN (SELECT restaurant_id FROM email_queue WHERE status != 'rejected')""")
            
            restaurants = cur.fetchall()
            queued = 0
            
            for r in restaurants:
                row = dict_row(r) if USE_POSTGRES else {'id': r[0], 'name': r[1], 'city': r[2], 'email': r[3]}
                
                # Skip if no valid email
                if not row.get('email') or '@' not in str(row.get('email', '')):
                    continue
                
                try:
                    if USE_POSTGRES:
                        cur.execute("""INSERT INTO email_queue (restaurant_id, restaurant_name, to_email, city, status)
                            VALUES (%s, %s, %s, %s, 'pending')""",
                            (row['id'], row['name'], row['email'], row.get('city', '')))
                    else:
                        cur.execute("""INSERT INTO email_queue (restaurant_id, restaurant_name, to_email, city, status)
                            VALUES (?, ?, ?, ?, 'pending')""",
                            (row['id'], row['name'], row['email'], row.get('city', '')))
                    queued += 1
                except:
                    pass
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify({'success': True, 'queued': queued})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/queue/<int:queue_id>/approve', methods=['POST'])
    def approve_email(queue_id):
        """Approve a single email for sending"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            if USE_POSTGRES:
                cur.execute("UPDATE email_queue SET status='approved', approved_at=NOW() WHERE id=%s", (queue_id,))
            else:
                cur.execute("UPDATE email_queue SET status='approved', approved_at=CURRENT_TIMESTAMP WHERE id=?", (queue_id,))
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/queue/<int:queue_id>/reject', methods=['POST'])
    def reject_email(queue_id):
        """Reject an email (won't be sent)"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            if USE_POSTGRES:
                cur.execute("UPDATE email_queue SET status='rejected' WHERE id=%s", (queue_id,))
            else:
                cur.execute("UPDATE email_queue SET status='rejected' WHERE id=?", (queue_id,))
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/queue/approve-bulk', methods=['POST'])
    def approve_emails_bulk():
        """Approve multiple emails at once"""
        try:
            data = request.json
            ids = data.get('ids', [])
            
            if not ids:
                return jsonify({'approved': 0})
            
            conn = get_db()
            cur = get_cursor(conn)
            
            approved = 0
            for qid in ids:
                try:
                    if USE_POSTGRES:
                        cur.execute("UPDATE email_queue SET status='approved', approved_at=NOW() WHERE id=%s", (qid,))
                    else:
                        cur.execute("UPDATE email_queue SET status='approved', approved_at=CURRENT_TIMESTAMP WHERE id=?", (qid,))
                    approved += 1
                except:
                    pass
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True, 'approved': approved})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/queue/reject-bulk', methods=['POST'])
    def reject_emails_bulk():
        """Reject multiple emails at once"""
        try:
            data = request.json
            ids = data.get('ids', [])
            
            if not ids:
                return jsonify({'rejected': 0})
            
            conn = get_db()
            cur = get_cursor(conn)
            
            rejected = 0
            for qid in ids:
                try:
                    if USE_POSTGRES:
                        cur.execute("UPDATE email_queue SET status='rejected' WHERE id=%s", (qid,))
                    else:
                        cur.execute("UPDATE email_queue SET status='rejected' WHERE id=?", (qid,))
                    rejected += 1
                except:
                    pass
            
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({'success': True, 'rejected': rejected})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/queue/<int:queue_id>/preview', methods=['GET'])
    def preview_email(queue_id):
        """Preview the email content"""
        try:
            conn = get_db()
            cur = get_cursor(conn)
            
            cur.execute("SELECT * FROM email_queue WHERE id=%s" if USE_POSTGRES else "SELECT * FROM email_queue WHERE id=?", (queue_id,))
            queue_item = cur.fetchone()
            
            if not queue_item:
                return jsonify({'error': 'Email not found'}), 404
            
            queue_data = dict_row(queue_item)
            
            # Get email template
            cur.execute("SELECT * FROM email_templates WHERE name='welcome' LIMIT 1")
            template_row = cur.fetchone()
            
            if template_row:
                template = dict_row(template_row)
                subject = template.get('subject', 'Congratulations!')
                body = template.get('body_html', '<p>Your restaurant has been featured!</p>')
            else:
                subject = 'Congratulations! Your restaurant is now featured on Spotwego'
                body = '''
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
    <div style="background: #6B4444; padding: 20px; text-align: center;">
        <h1 style="color: white; margin: 0; font-family: Georgia, serif; font-style: italic;">Spotwego</h1>
    </div>
    <div style="padding: 30px; background: #FAF7F5;">
        <h2 style="color: #3D2929;">Congratulations, {{contact_name}}!</h2>
        <p style="color: #3D2929;">We're excited to inform you that <strong>{{restaurant_name}}</strong> has been selected to be featured on Spotwego!</p>
    </div>
    <div style="background: #6B4444; padding: 15px; text-align: center; color: #C4A4A4; font-size: 12px;">
        © 2025 Spotwego. All rights reserved.
    </div>
</body>
</html>'''
            
            # Replace variables
            html = body.replace('{{restaurant_name}}', queue_data.get('restaurant_name', 'Restaurant'))
            html = html.replace('{{contact_name}}', 'Restaurant Manager')
            html = html.replace('{{city}}', queue_data.get('city', ''))
            html = html.replace('{{address}}', '')
            
            cur.close()
            conn.close()
            
            return jsonify({'html': html, 'subject': subject})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/email/send-approved', methods=['POST'])
    def send_approved_emails():
        """Send all approved emails from the queue"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            conn = get_db()
            cur = get_cursor(conn)
            
            # Get email config
            try:
                cur.execute("SELECT * FROM email_config WHERE is_active=1 LIMIT 1")
                config_row = cur.fetchone()
            except:
                config_row = None
            
            if not config_row:
                return jsonify({'error': 'Email configuration not found. Please configure SMTP settings first.'}), 400
            
            config = dict_row(config_row)
            
            if not config.get('smtp_host') or not config.get('smtp_user') or not config.get('smtp_password'):
                return jsonify({'error': 'Incomplete SMTP configuration.'}), 400
            
            # Get approved emails
            cur.execute("SELECT * FROM email_queue WHERE status='approved'")
            approved_emails = [dict_row(r) for r in cur.fetchall()]
            
            if not approved_emails:
                return jsonify({'sent': 0, 'failed': 0, 'message': 'No approved emails to send'})
            
            # Get email template
            try:
                cur.execute("SELECT * FROM email_templates WHERE name='welcome' LIMIT 1")
                template_row = cur.fetchone()
                template = dict_row(template_row) if template_row else None
            except:
                template = None
            
            if template:
                subject = template.get('subject', 'Congratulations!')
                body_html = template.get('body_html', '<p>Your restaurant has been featured!</p>')
            else:
                subject = 'Congratulations! Your restaurant is now featured on Spotwego'
                body_html = '''<html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
    <div style="background: #6B4444; padding: 20px; text-align: center;">
        <h1 style="color: white; margin: 0; font-family: Georgia, serif; font-style: italic;">Spotwego</h1>
    </div>
    <div style="padding: 30px; background: #FAF7F5;">
        <h2 style="color: #3D2929;">Congratulations!</h2>
        <p style="color: #3D2929;">Your restaurant <strong>{{restaurant_name}}</strong> has been selected to be featured on Spotwego!</p>
    </div>
    <div style="background: #6B4444; padding: 15px; text-align: center; color: #C4A4A4; font-size: 12px;">
        © 2025 Spotwego. All rights reserved.
    </div>
</body></html>'''
            
            # Send emails
            results = {'sent': 0, 'failed': 0, 'errors': []}
            
            try:
                server = smtplib.SMTP(config['smtp_host'], int(config.get('smtp_port', 587)))
                server.starttls()
                server.login(config['smtp_user'], config['smtp_password'])
                
                for email_item in approved_emails:
                    try:
                        # Prepare email
                        msg = MIMEMultipart('alternative')
                        msg['Subject'] = subject.replace('{{restaurant_name}}', email_item.get('restaurant_name', 'Restaurant'))
                        msg['From'] = f"{config.get('from_name', 'Spotwego')} <{config['smtp_user']}>"
                        msg['To'] = email_item['to_email']
                        
                        # Replace variables in body
                        html = body_html.replace('{{restaurant_name}}', email_item.get('restaurant_name', 'Restaurant'))
                        html = html.replace('{{contact_name}}', 'Restaurant Manager')
                        html = html.replace('{{city}}', email_item.get('city', ''))
                        html = html.replace('{{address}}', '')
                        
                        msg.attach(MIMEText(html, 'html'))
                        
                        # Send
                        server.sendmail(config['smtp_user'], email_item['to_email'], msg.as_string())
                        
                        # Update queue status
                        if USE_POSTGRES:
                            cur.execute("UPDATE email_queue SET status='sent', sent_at=NOW() WHERE id=%s", (email_item['id'],))
                        else:
                            cur.execute("UPDATE email_queue SET status='sent', sent_at=CURRENT_TIMESTAMP WHERE id=?", (email_item['id'],))
                        
                        # Log the email
                        try:
                            if USE_POSTGRES:
                                cur.execute("""INSERT INTO email_log (restaurant_id, to_email, subject, status, sent_at)
                                    VALUES (%s, %s, %s, 'sent', NOW())""",
                                    (email_item.get('restaurant_id'), email_item['to_email'], subject))
                            else:
                                cur.execute("""INSERT INTO email_log (restaurant_id, to_email, subject, status, sent_at)
                                    VALUES (?, ?, ?, 'sent', CURRENT_TIMESTAMP)""",
                                    (email_item.get('restaurant_id'), email_item['to_email'], subject))
                        except:
                            pass
                        
                        results['sent'] += 1
                        
                    except Exception as e:
                        results['failed'] += 1
                        results['errors'].append({'to': email_item['to_email'], 'error': str(e)})
                
                server.quit()
                
            except smtplib.SMTPAuthenticationError as e:
                return jsonify({'error': f'SMTP Authentication failed: {str(e)}'}), 400
            except Exception as e:
                return jsonify({'error': f'SMTP error: {str(e)}'}), 400
            
            conn.commit()
            cur.close()
            conn.close()
            
            return jsonify(results)
            
        except Exception as e:
            import traceback
            app.logger.error(f'Send approved emails error: {traceback.format_exc()}')
            return jsonify({'error': str(e)}), 500


# ============================================================================
# COMPUTE RANKINGS
# ============================================================================

def compute_rankings(region_code):
    """Compute rankings for restaurants in a region.
    Since rankings table has no region column, we:
    1. Find restaurants by canton/city
    2. Compute/update their global ranking entry
    3. Re-rank all restaurants globally by score
    """
    from flask import current_app
    current_app.logger.info(f"compute_rankings called for region: {region_code}")
    
    conn = get_db()
    cur = get_cursor(conn)
    p = '%s' if USE_POSTGRES else '?'
    
    # Get all restaurant IDs in region - match by canton or city (case-insensitive)
    # Also try matching by region name
    try:
        # First get region name
        if USE_POSTGRES:
            cur.execute("SELECT name FROM regions WHERE code=%s", (region_code,))
        else:
            cur.execute("SELECT name FROM regions WHERE code=?", (region_code,))
        region_row = cur.fetchone()
        region_name = region_row[0] if region_row else region_code
        
        current_app.logger.info(f"Region name: {region_name}")
        
        if USE_POSTGRES:
            cur.execute("""SELECT id, name, canton, city FROM restaurants 
                WHERE is_active=1 AND (
                    canton=%s OR LOWER(canton)=LOWER(%s) OR 
                    LOWER(city)=LOWER(%s) OR LOWER(city)=LOWER(%s)
                )""", 
                (region_code, region_code, region_code, region_name))
        else:
            cur.execute("""SELECT id, name, canton, city FROM restaurants 
                WHERE is_active=1 AND (
                    canton=? OR LOWER(canton)=LOWER(?) OR 
                    LOWER(city)=LOWER(?) OR LOWER(city)=LOWER(?)
                )""", 
                (region_code, region_code, region_code, region_name))
        
        rows = cur.fetchall()
        ids = [r['id'] if isinstance(r, dict) else r[0] for r in rows]
        
        current_app.logger.info(f"Found {len(ids)} restaurants for region {region_code}")
        if rows and len(rows) <= 5:
            for r in rows:
                current_app.logger.info(f"  - {r}")
                
    except Exception as e:
        current_app.logger.error(f"Error finding restaurants: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        ids = []
    
    cur.close()
    conn.close()
    
    if ids:
        current_app.logger.info(f"Computing rankings for {len(ids)} restaurants")
        compute_rankings_for_restaurants(ids)
    else:
        current_app.logger.warning(f"No restaurants found for region {region_code}")


def compute_rankings_for_restaurants(restaurant_ids):
    """
    Compute scores for specified restaurants and update their rankings.
    Rankings are global (not per-region) since the rankings table has no region column.
    """
    conn = get_db()
    cur = get_cursor(conn)
    p = '%s' if USE_POSTGRES else '?'
    
    # Get weights
    cur.execute("SELECT * FROM ranking_weights WHERE is_active=1 LIMIT 1")
    wr = cur.fetchone()
    w = dict_row(wr) if wr else {
        'weight_google': 0.25, 'weight_yelp': 0.20, 'weight_tripadvisor': 0.25,
        'weight_user_reviews': 0.15, 'weight_sentiment': 0.10, 'weight_data_quality': 0.05,
        'min_reviews_threshold': 5
    }
    
    custom_weights = {}
    if w.get('custom_weights'):
        try:
            custom_weights = json.loads(w['custom_weights'])
        except:
            pass
    
    provider_weights = {
        'google': w.get('weight_google', 0.25),
        'yelp': w.get('weight_yelp', 0.20),
        'tripadvisor': w.get('weight_tripadvisor', 0.25)
    }
    provider_weights.update(custom_weights)
    
    # Compute scores for each restaurant
    for rid in restaurant_ids:
        try:
            # Source ratings
            cur.execute(f"SELECT source, avg_rating, review_count FROM source_ratings WHERE restaurant_id={p}", (rid,))
            sources = {}
            for r in cur.fetchall():
                row = dict_row(r) if hasattr(r, 'keys') else {'source': r[0], 'avg_rating': r[1], 'review_count': r[2]}
                sources[row['source']] = {'rating': row['avg_rating'], 'count': row['review_count'] or 0}
            
            # Sentiment from reviews
            try:
                cur.execute(f"SELECT AVG(sentiment_score) as s, COUNT(*) as c FROM reviews WHERE restaurant_id={p} AND sentiment_score IS NOT NULL", (rid,))
                sr = cur.fetchone()
                sr = dict_row(sr) if sr else {}
                avg_sent = sr.get('s') or 0
                sent_count = sr.get('c') or 0
            except:
                avg_sent = 0
                sent_count = 0
            
            # Calculate composite score
            total_reviews = sum(s.get('count', 0) for s in sources.values())
            weighted_rating = 0
            total_weight = 0
            
            for source, sw in provider_weights.items():
                if source in sources and sources[source].get('rating'):
                    weighted_rating += sources[source]['rating'] * sw
                    total_weight += sw
            
            avg_rating = weighted_rating / total_weight if total_weight > 0 else 0
            
            sentiment_contrib = ((avg_sent + 1) / 2) * 5 * w.get('weight_sentiment', 0.10)
            remaining_weight = 1 - w.get('weight_sentiment', 0.10)
            composite = (avg_rating * remaining_weight + sentiment_contrib) if avg_rating > 0 else 0
            
            min_threshold = w.get('min_reviews_threshold', 5)
            confidence = min(1.0, math.log10(total_reviews + 1) / math.log10(100)) if total_reviews >= min_threshold else 0.5
            
            # Check if ranking exists (rankings are per-restaurant, not per-region)
            if USE_POSTGRES:
                cur.execute("SELECT id FROM rankings WHERE restaurant_id=%s LIMIT 1", (rid,))
            else:
                cur.execute("SELECT id FROM rankings WHERE restaurant_id=? LIMIT 1", (rid,))
            
            existing_rank = cur.fetchone()
            
            if existing_rank:
                # Update existing ranking - only use columns that exist
                rank_id = existing_rank['id'] if isinstance(existing_rank, dict) else existing_rank[0]
                if USE_POSTGRES:
                    cur.execute("""UPDATE rankings SET
                        composite_score=%s, sentiment_avg=%s, confidence_score=%s, 
                        total_reviews=%s, last_computed=NOW()
                        WHERE id=%s""",
                        (round(composite, 3), round(avg_sent, 3) if avg_sent else 0,
                         round(confidence, 3), total_reviews, rank_id))
                else:
                    cur.execute("""UPDATE rankings SET
                        composite_score=?, sentiment_avg=?, confidence_score=?, 
                        total_reviews=?, last_computed=CURRENT_TIMESTAMP
                        WHERE id=?""",
                        (round(composite, 3), round(avg_sent, 3) if avg_sent else 0,
                         round(confidence, 3), total_reviews, rank_id))
            else:
                # Insert new ranking - only use columns that exist
                if USE_POSTGRES:
                    cur.execute("""INSERT INTO rankings 
                        (restaurant_id, composite_score, sentiment_avg, confidence_score, total_reviews, auto_rank, is_featured, is_published)
                        VALUES (%s, %s, %s, %s, %s, 0, 0, 0)""",
                        (rid, round(composite, 3), round(avg_sent, 3) if avg_sent else 0,
                         round(confidence, 3), total_reviews))
                else:
                    cur.execute("""INSERT INTO rankings 
                        (restaurant_id, composite_score, sentiment_avg, confidence_score, total_reviews, auto_rank, is_featured, is_published)
                        VALUES (?, ?, ?, ?, ?, 0, 0, 0)""",
                        (rid, round(composite, 3), round(avg_sent, 3) if avg_sent else 0,
                         round(confidence, 3), total_reviews))
            
            conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except:
                pass
    
    # Re-rank ALL restaurants globally by composite score
    try:
        cur.execute("SELECT id, restaurant_id, composite_score FROM rankings ORDER BY composite_score DESC")
        all_rankings = cur.fetchall()
        
        for i, row in enumerate(all_rankings):
            row = dict_row(row) if hasattr(row, 'keys') else {'id': row[0], 'restaurant_id': row[1]}
            rank_id = row['id']
            rank = i + 1
            if USE_POSTGRES:
                cur.execute("UPDATE rankings SET auto_rank=%s WHERE id=%s", (rank, rank_id))
            else:
                cur.execute("UPDATE rankings SET auto_rank=? WHERE id=?", (rank, rank_id))
        
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
    
    cur.close()
    conn.close()


# Keep old function name for compatibility
def compute_rankings_selective(region_code, restaurant_ids):
    """Wrapper for backward compatibility"""
    compute_rankings_for_restaurants(restaurant_ids)


# ============================================================================
# APP
# ============================================================================

app = create_app()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    print(f"\n{'='*50}")
    print(f"  Spotwego Restaurant Dashboard V4.2")
    print(f"  Database: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
    if not USE_POSTGRES:
        print(f"  SQLite Path: {Config.SQLITE_PATH}")
    print(f"  URL: http://localhost:{port}")
    print(f"{'='*50}\n")
    app.run(debug=os.environ.get('FLASK_ENV') == 'development', host='0.0.0.0', port=port)
