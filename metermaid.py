import os
import sys
import math
import time
import argparse
import subprocess
import contextlib
import sqlite3
import json
import re
from datetime import datetime


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--database", default="meter.db", help="Optionally specify the path to the meter database")
	args = parser.parse_args()

	print("Starting up rtl_tcp...")
	with subprocess.Popen("rtl_tcp", stdout=subprocess.PIPE, stderr=subprocess.PIPE) as rtltcp_proc:
		time.sleep(3)
		rtltcp_proc.poll()
		if rtltcp_proc.returncode is not None:
			print("Could not initialize rtl_tcp, exiting...")
			return
		print("rtl_tcp initialized successfully")

		print("Starting up rtlamr...")
		with subprocess.Popen(["rtlamr", "-msgtype=scm+", "-format=json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE) as rtlamr_proc:
			time.sleep(3)
			rtltcp_proc.poll()
			if rtlamr_proc.returncode is not None:
				print("Could not initialize rtlamr, exiting...")
				return
			print("rtlamr initialized succesfully")

			print("Restarting in process meter database...")
			connection = sqlite3.connect(args.database)
			initialize_database_schema(connection)
			print("Database initialized successfully")
			print("Consuming available meter data...")
			consume_rtlamr(rtlamr_proc, connection)


def consume_rtlamr(rtlamr_proc, connection):
	latest_readings = {}
	cursor = connection.cursor()
	while rtlamr_proc.returncode is None:
		stdout = rtlamr_proc.stdout.readline()
		if stdout is not None:
			print("Received stdout -> {0}".format(stdout))
			reading_json = json.loads(stdout.decode())
			reading_time = reading_json["Time"]
			if ":" == reading_time[-3]:
				reading_time = reading_time[:-3] + reading_time[-2:]
			reading_time_trunc = re.sub("\.\d+", "", reading_time)
			reading_datetime = datetime.strptime(reading_time_trunc, "%Y-%m-%dT%H:%M:%S%z")
			reading_epoch = math.floor(reading_datetime.timestamp())
			reading_type = reading_json["Type"]
			reading_endpoint_id = reading_json["Message"]["EndpointID"]
			reading_value = reading_json["Message"]["Consumption"]
			reading_record = {
				"datetime": reading_epoch,
				"type": reading_type,
				"endpoint_id": reading_endpoint_id,
				"reading": reading_value
			}

			reading_key = "{0}-{1}".format(reading_type, reading_endpoint_id)
			if reading_key in latest_readings:
				is_duplicate_reading = reading_value == latest_readings[reading_key]["reading"]
				is_within_time_interval = (reading_epoch - latest_readings[reading_key]["datetime"]) < 900
				if is_duplicate_reading and is_within_time_interval:
					print("Received duplicate reading within allotted time interval, skipping persistence")
					continue
			latest_readings[reading_key] = reading_record

			cursor.execute("""
				INSERT INTO meter_readings (
					datetime, type, endpoint_id, reading
				) values (
					:datetime, :type, :endpoint_id, :reading
				);""", reading_record)
			connection.commit()
		rtlamr_proc.poll()
	print("Process rtlamr exited with return code {0}".format(rtlamr_proc.returncode))


def initialize_database_schema(connection):
	cursor = connection.cursor()
	cursor.execute("""
		CREATE TABLE IF NOT EXISTS meter_readings (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			datetime INTEGER NOT NULL,
			type TEXT NOT NULL,
			endpoint_id INTEGER NOT NULL,
			reading REAL NOT NULL
		);
	""")


# Doesn't work -- not sure why
def unbuffered(proc, stream='stdout'):
	newlines = ['\n', '\r\n', '\r']
	stream = getattr(proc, stream)
	with contextlib.closing(stream):
		while True:
			out = []
			last = stream.read(1)
			# Don't loop forever
			if last == '' and proc.poll() is not None:
				break
			while last not in newlines:
				# Don't loop forever
				if last == '' and proc.poll() is not None:
					break
				out.append(last)
				last = stream.read(1)
			out = ''.join(out)
			yield out


if __name__ == '__main__':
	main()
