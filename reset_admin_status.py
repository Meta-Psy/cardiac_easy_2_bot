# -*- coding: utf-8 -*-
"""
Script to reset broadcast/followup status for administrators
Run: python reset_admin_status.py
"""
import sys
import os
import io

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add bot modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from database import get_db_sync, FollowUpStatus, FollowUpSurvey

# Admin IDs from .env
ADMIN_IDS = [71353121, 173760998, 491851234]

def reset_admin_status():
    """Reset broadcast/followup status for administrators"""
    db = get_db_sync()

    try:
        for admin_id in ADMIN_IDS:
            # Delete FollowUpStatus record
            status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == admin_id).first()
            if status:
                db.delete(status)
                print(f"[OK] Deleted FollowUpStatus for {admin_id}")
            else:
                print(f"[INFO] No FollowUpStatus found for {admin_id}")

            # Delete FollowUpSurvey record
            survey = db.query(FollowUpSurvey).filter(FollowUpSurvey.telegram_id == admin_id).first()
            if survey:
                db.delete(survey)
                print(f"[OK] Deleted FollowUpSurvey for {admin_id}")
            else:
                print(f"[INFO] No FollowUpSurvey found for {admin_id}")

        db.commit()

        print("\n" + "="*50)
        print("[OK] All admin statuses reset successfully!")
        print("="*50)
        print(f"\nReset status for {len(ADMIN_IDS)} admins:")
        for admin_id in ADMIN_IDS:
            print(f"  - {admin_id}")
        print("\nYou can now re-send broadcasts to these users")

    except Exception as e:
        db.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    print("="*50)
    print("Resetting broadcast status for administrators")
    print("="*50 + "\n")
    reset_admin_status()
