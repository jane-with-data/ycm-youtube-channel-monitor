from cryptography.fernet import Fernet

import secrets

print('# Copy paste vào .env:')

print()

print('POSTGRES_PASSWORD=' + secrets.token_urlsafe(16))

print('AIRFLOW_ADMIN_PASSWORD=' + secrets.token_urlsafe(16))

print('AIRFLOW_ADMIN_EMAIL=admin@dwh.local')

print('AIRFLOW_FERNET_KEY=' + Fernet.generate_key().decode())

print('AIRFLOW_SECRET_KEY=' + secrets.token_hex(32))

