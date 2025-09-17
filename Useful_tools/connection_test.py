#!/usr/bin/env python3
"""
connection_test.py

Reads DATABASE_URL (or DB_URL) from environment or .env and runs a lightweight
SELECT 1 to validate connectivity and credentials.

Usage (PowerShell):
  python .\connection_test.py

Exit codes:
 0 - success
 1 - connection or unexpected error
 2 - missing DATABASE_URL
"""

import os
import re
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def mask_db_url(u: str) -> str:
	# mask password for display
	return re.sub(r'://([^:@]+)(:[^@]+)?@', r'://\1:*****@', u)


def main():
	load_dotenv()

	db_url = os.getenv('DATABASE_URL') or os.getenv('DB_URL')
	if not db_url:
		print('ERROR: No DATABASE_URL (or DB_URL) found in environment or .env file.')
		sys.exit(2)

	print('Using DATABASE_URL (masked):', mask_db_url(db_url))

	try:
		engine = create_engine(db_url, connect_args={"connect_timeout": 5})
		with engine.connect() as conn:
			conn.execute(text('SELECT 1'))

		print('Connection test: SUCCESS')
		sys.exit(0)

	except SQLAlchemyError as e:
		print('Connection test: FAILED')
		print('SQLAlchemy error:', e)
		sys.exit(1)
	except Exception as e:
		print('Connection test: FAILED')
		print('Unexpected error:', e)
		sys.exit(1)


if __name__ == '__main__':
	main()

