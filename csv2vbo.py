#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 Lubomir Bulej <pallas@kadan.cz>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import csv
import sys
import time

from abc import ABCMeta
from decimal import Decimal
from datetime import datetime
from functools import partial
from collections import OrderedDict


class DataFrame(object):
	def __init__(self, head, data, info):
		self._head_row = head
		self._data_rows = data
		self._info_rows = info

	def rows(self):
		return self._data_rows

	def comments(self):
		return self._info_rows

	def header(self):
		return self._head_row


class Converter(object):
	__metaclass__ = ABCMeta

	def __init__(self):
		self._name_map = {}
		self._value_map = {
			"satellites": lambda v: "%03d" % Decimal(v),
			"time": lambda v: self._format_decimal_secs(Decimal(v)),
			# Convert latitude and longitude to angular minutes.
			"latitude": lambda v: "%+012.5f" % (60 * Decimal(v)),
			"longitude": lambda v: "%+012.5f" % (-60 * Decimal(v)),
			"velocity kmh": lambda v: "%07.3f" % Decimal(v),
			"heading": lambda v: "%06.2f" % Decimal(v),
			"height": lambda v: "%+09.2f" % Decimal(v),
		}

	@staticmethod
	def _format_decimal_secs(secs):
		int_secs = int(secs)
		return "%s.%02d" % (
			# Convert the integral seconds to HMS.
			time.strftime("%H%M%S", time.gmtime(int_secs)),
			# Round the fractional part of a second to milliseconds.
			(100 * (secs - int_secs)).to_integral()
		)

	def supported_types(self):
		"""Returns the supported VBO data types."""
		return self._name_map.values()

	def recognizes(self, row):
		"""Checks whether this converter recognizes the header row."""
		return all([item in row for item in self._name_map.keys()])

	def convert(self, csv_data):
		"""Converts a CSV data frame to VBO data frame."""
		vbo_rows = []
		for csv_row in csv_data.rows():
			vbo_row = {}
			for (csv_name, csv_value) in zip(csv_data.header(), csv_row):
				vbo_name = self._name_map.get(csv_name)
				if vbo_name is not None:
					mapper = self._value_map.get(vbo_name)
					if mapper is not None:
						vbo_row[vbo_name] = mapper(csv_value)
					else:
						raise Exception("no mapper for %s in %s" %
							(vbo_name, type(self).__name__)
						)

			# Convert the dictionary to a list with a particular item order.
			vbo_rows.append([vbo_row[name] for name in self.supported_types()])

		return DataFrame(
			head=self.supported_types(), data=vbo_rows, info=csv_data.comments()
		)


class RaceChronoConverter(Converter):

	def __init__(self):
		super(RaceChronoConverter, self).__init__()

		self._name_map = {
			"Locked satellites": "satellites",
			"Timestamp (s)": "time",
			"Latitude (deg)": "latitude",
			"Longitude (deg)": "longitude",
			"Speed (kph)": "velocity kmh",
			"Bearing (deg)": "heading",
			"Altitude (m)": "height"
		}


class GTechFanaticConverter(Converter):

	def __init__(self):
		super(GTechFanaticConverter, self).__init__()

		self._name_map = {
			"Time(s)": "time",
			"GPS_Lat": "latitude",
			"GPS_Lon": "longitude",
			"Speed(kph)": "velocity kmh",
			"Heading(deg)": "heading"
		}

		self._value_map.update({
			"latitude": lambda v: "%+012.5f" % (Decimal(v) / 10000),
			"longitude": lambda v: "%+012.5f" % (-Decimal(v) / 10000)
		})


class TrackMasterConverter(Converter):

	def __init__(self):
		super(TrackMasterConverter, self).__init__()

		self._name_map = {
			"time=": "time",
			"latitude=": "latitude",
			"longitude=": "longitude",
			"speed=": "velocity kmh",
			"bearing=": "heading",
			"altitude=": "height"
		}

		self._value_map.update({
			"time": lambda v:
				self._format_decimal_secs(self._datetime_to_secs(v))
		})

	@staticmethod
	def _datetime_to_secs(value):
		return Decimal(
			datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
			.strftime("%H%M%S.%f")
		)


def read_csv(input):
	reader = csv.reader(csv_input, delimiter=",", quotechar='"')

	# Filter out empty rows and strip all space around data items.
	rows = [[x.strip() for x in row] for row in reader if len(row) > 0]

	head_row = max(rows, key=len)

	# Split the data at the header into info rows and data rows
	head_index = rows.index(head_row)
	info_rows = rows[0:head_index]
	data_rows = rows[head_index + 1:]

	return DataFrame(head=head_row, data=data_rows, info=info_rows)


def find_converter(data):
	converters = (
		RaceChronoConverter(),
		GTechFanaticConverter(),
		TrackMasterConverter()
	)

	for converter in converters:
		if converter.recognizes(data.header()):
			return converter

	return None


def write_vbo(vbo_data, vbo_output):
	# User ordered dict here to maintain the usual VBO column order.
	vbo_cols = OrderedDict((
		("satellites", "sats"), ("time", "time"),
		("latitude", "lat"), ("longitude", "long"),
		("velocity kmh", "velocity"), ("heading", "heading"),
		("height", "height")
	))

	# Curry print() with the output file and MS-DOS line suffix.
	output = partial(print, file=vbo_output, end="\r\n")

	output(time.strftime("File created on %d/%m/%Y at %I:%M:%S %p"))

	# Only output VBO columns provided by the converter.
	out_cols = OrderedDict((
		(k, v) for (k, v) in vbo_cols.items()
			if k in vbo_data.header()
	))

	output("\r\n[header]")
	for data_type in out_cols.keys():
		output(data_type)

	output("\r\n[comments]")
	for row in vbo_data.comments():
		if len(row) == 1:
			output(row[0])
		elif len(row) > 1 and len("".join(row[1:])) > 0:
			output("%s : %s" % (row[0], ";".join(row[1:])))

	output("\r\n[column names]")
	output(" ".join(out_cols.values()))

	# Get a map of indices to output data in the usual VBO order.
	indices = dict([(n, i) for (i, n) in enumerate(vbo_data.header())])

	output("\r\n[data]")
	for row in vbo_data.rows():
		out_row = [row[indices.get(col)] for col in out_cols.keys()]
		output(" ".join(out_row))


### Read the CSV input, find a suitable converter,
### convert the CSV data to VBO data, and write the VBO output.

with sys.stdin as csv_input:
	csv_data = read_csv(csv_input)

converter = find_converter(csv_data)
if converter is None:
	print ("error: unable to recognize input format", file=sys.stderr)
	sys.exit(-1)

vbo_data = converter.convert(csv_data)

with sys.stdout as vbo_output:
	write_vbo(vbo_data, vbo_output)
