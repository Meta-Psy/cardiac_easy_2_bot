# -*- coding: utf-8 -*-
"""
Script to add test data for administrators
Run: python add_test_data.py
"""
import sys
import os
import io

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add bot modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from datetime import datetime
from database import get_db_sync, User, Survey, TestResult

# Admin IDs from .env
ADMIN_IDS = [71353121, 173760998, 491851234]

def add_test_data():
    """Add test data for administrators"""
    db = get_db_sync()

    try:
        for i, admin_id in enumerate(ADMIN_IDS):
            # Check if user exists
            existing_user = db.query(User).filter(User.telegram_id == admin_id).first()

            if existing_user:
                print(f"User {admin_id} exists, updating...")
                user = existing_user
            else:
                print(f"Creating new user {admin_id}...")
                user = User(telegram_id=admin_id)
                db.add(user)

            # Update user data
            user.name = f"Test Admin {i+1}"
            user.email = f"admin{i+1}@test.com"
            user.phone = f"+7900000000{i+1}"
            user.registration_completed = True
            user.completed_diagnostic = True
            user.survey_completed = True
            user.tests_completed = True
            user.webinar_watched = True
            user.last_activity = datetime.utcnow()

            db.commit()

            # Check if Survey exists
            existing_survey = db.query(Survey).filter(Survey.telegram_id == admin_id).first()

            if existing_survey:
                print(f"  Survey for {admin_id} exists, updating...")
                survey = existing_survey
            else:
                print(f"  Creating survey for {admin_id}...")
                survey = Survey(telegram_id=admin_id)
                db.add(survey)

            # Fill survey data
            survey.age = 35 + i * 5
            survey.gender = ["Male", "Female", "Male"][i]
            survey.location = "Moscow"
            survey.education = "Higher"
            survey.family_status = "Married"
            survey.children = "Yes"
            survey.income = "50 000 - 100 000"
            survey.health_rating = 7
            survey.death_cause = "Cardiovascular diseases"
            survey.heart_disease = "No"
            survey.cv_risk = "Low"
            survey.cv_knowledge = "Medium"
            survey.health_importance = "Very important"
            survey.completed_at = datetime.utcnow()

            db.commit()

            # Check if TestResult exists
            existing_test = db.query(TestResult).filter(TestResult.telegram_id == admin_id).first()

            if existing_test:
                print(f"  Test results for {admin_id} exist, updating...")
                test_result = existing_test
            else:
                print(f"  Creating test results for {admin_id}...")
                test_result = TestResult(telegram_id=admin_id)
                db.add(test_result)

            # Fill test results
            test_result.hads_anxiety_score = 5 + i
            test_result.hads_depression_score = 4 + i
            test_result.hads_anxiety_level = "normal"
            test_result.hads_depression_level = "normal"
            test_result.burns_score = 10 + i * 2
            test_result.burns_level = "minimal"
            test_result.isi_score = 6 + i
            test_result.isi_level = "subthreshold"
            test_result.overall_cv_risk_level = ["LOW", "MODERATE", "LOW"][i]
            test_result.overall_cv_risk_score = 2 + i
            test_result.completed_at = datetime.utcnow()

            db.commit()

            print(f"[OK] Data for admin {admin_id} added successfully!")

        print("\n" + "="*50)
        print("[OK] All test data added successfully!")
        print("="*50)
        print(f"\nAdded data for {len(ADMIN_IDS)} admins:")
        for admin_id in ADMIN_IDS:
            print(f"  - {admin_id}")
        print("\nYou can now test surveys (POINT 2 and POINT 3)")

    except Exception as e:
        db.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    print("="*50)
    print("Adding test data for administrators")
    print("="*50 + "\n")
    add_test_data()
