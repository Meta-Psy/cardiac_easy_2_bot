import sys
import os
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from database import get_db_sync, User

db = get_db_sync()
count = db.query(User).count()
registered = db.query(User).filter(User.registration_completed == True).count()

print(f'Total users: {count}')
print(f'Registered users: {registered}')

if registered > 0:
    user = db.query(User).filter(User.registration_completed == True).first()
    print(f'\nExample user for testing:')
    print(f'  Telegram ID: {user.telegram_id}')
    print(f'  Name: {user.name}')

db.close()
