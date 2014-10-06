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

from abc import ABCMeta
from collections import OrderedDict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, time
from functools import partial
from math import ceil


class DataFrame(object):
	def __init__(self, head, data, info, units={}):
		self._head_row = head
		self._data_rows = data
		self._info_rows = info
		self._head_units = units

	def rows(self):
		return self._data_rows

	def comments(self):
		return self._info_rows

	def header(self):
		return self._head_row

	def units(self):
		return self._head_units


class Converter(object):
	__metaclass__ = ABCMeta

	def __init__(self):
		self._base_map = {}
		self._user_map = {}

		self._value_map = {
			"satellites": lambda v: Decimal(v),
			"time": lambda v: Decimal(v),
			# Convert latitude and longitude to angular minutes.
			"latitude": lambda v: 60 * Decimal(v),
			"longitude": lambda v: -60 * Decimal(v),
			"velocity kmh": lambda v: Decimal(v),
			"heading": lambda v: Decimal(v),
			"height": lambda v: Decimal(v),
			# Often used user-defined channels.
			"LatAcc": lambda v: self._decimal_or_default(v, 0.0),
			"LongAcc": lambda v: self._decimal_or_default(v, 0.0),
		}

	@staticmethod
	def _decimal_or_default(value, default):
		try:
			return Decimal(value)
		except:
			return Decimal(default)

	def base_types(self):
		"""Returns the list of supported RaceLogic base data types."""
		return self._base_map.values()

	def user_types(self):
		"""Returns a dictionary of supported (user-defined) channel types."""
		return self._user_map

	def recognizes(self, row):
		"""Checks whether this converter recognizes the header row."""
		return all([item in row for item in self._base_map.keys()])

	def _map_name(self, csv_name):
		result = self._base_map.get(csv_name)
		if result is None:
			channel = self._user_map.get(csv_name)
			if channel is not None:
				result = channel[0]

		return result

	def _get_mapper(self, vbo_name):
		if vbo_name is None:
			# No mapper is expected in this case.
			return None

		mapper = self._value_map.get(vbo_name)
		if mapper is not None:
			return (vbo_name, mapper)
		else:
			raise Exception(
				"no mapper for %s in %s" % (vbo_name, type(self).__name__)
			)

	def _map_value(self, csv_value, mapper):
		try:
			return mapper[1](csv_value)

		except Exception as ex:
			# Warn about the problem, but return something relatively usable.
			print(
				"warning: failed to convert '%s' to %s\nexception: %s" %
				(csv_value, mapper[0], ex), file=sys.stderr
			)
			return "-0"

	def _map_values(self, csv_values, mappers):
		for (csv_value, mapper) in zip(csv_values, mappers):
			if mapper is not None:
				yield self._map_value(csv_value, mapper)

	def convert(self, csv_data):
		"""Converts a CSV data frame to VBO data frame."""
		# Map names from CSV header to VBO names (or None if unsupported).
		vbo_names = [self._map_name(name) for name in csv_data.header()]

		# Get the VBO header consisting only of supported fields.
		vbo_head = [x for x in vbo_names if x is not None]

		# CircuitTools requires the VBO data to contain the "satellites"
		# channel. Some loggers don't provide it, so we fudge it.
		base_row = []
		if "satellites" not in vbo_head:
			vbo_head.insert(0, "satellites")
			base_row.append(Decimal(5))

		# Collect CSV-to-VBO value mappers for supported VBO data types.
		mappers = [self._get_mapper(name) for name in vbo_names]

		# Map CSV values to VBO values row by row and eliminate potentially
		# duplicate adjacent rows resulting from mapping a subset of fields.
		vbo_rows = []
		last_row = None
		for csv_row in csv_data.rows():
			vbo_row = base_row.copy()
			vbo_row.extend(self._map_values(csv_row, mappers))
			if vbo_row != last_row:
				vbo_rows.append(vbo_row)
				last_row = vbo_row

		# Determine units for user-defined data types.
		vbo_units = {}
		for csv_name in csv_data.header():
			if csv_name in self._user_map:
				(vbo_name, unit) = self._user_map.get(csv_name)
				if vbo_name in vbo_head:
					vbo_units[vbo_name] = unit

		return DataFrame(
			head=vbo_head, data=vbo_rows,
			info=csv_data.comments(), units=vbo_units
		)


class RaceChronoConverter(Converter):

	def __init__(self):
		super(RaceChronoConverter, self).__init__()

		self._base_map = {
			"Locked satellites": "satellites",
			"Timestamp (s)": "time",
			"Latitude (deg)": "latitude",
			"Longitude (deg)": "longitude",
			"Speed (kph)": "velocity kmh",
			"Bearing (deg)": "heading",
			"Altitude (m)": "height"
		}

		self._user_map = {
			"Lateral Acceleration (G)": ("LatAcc", "m/s2"),
			"Longitudinal Acceleration (G)": ("LongAcc", "m/s2")
		}


class GTechFanaticConverter(Converter):

	def __init__(self):
		super(GTechFanaticConverter, self).__init__()

		self._base_map = {
			"Time(s)": "time",
			"GPS_Lat": "latitude",
			"GPS_Lon": "longitude",
			"Speed(kph)": "velocity kmh",
			"Heading(deg)": "heading"
		}

		self._user_map = {
			"G-Force_Lat(G)": ("LatAcc", "m/s2"),
			"G-Force_Fwd(G)": ("LongAcc", "m/s2")
		}

		self._value_map.update({
			"latitude": lambda v: Decimal(v) / 10000,
			"longitude": lambda v: -Decimal(v) / 10000,
			# G-Tech flips the sign on lateral acceleration.
			"LatAcc": lambda v: -self._decimal_or_default(v, 0.0),
		})


class TrackMasterConverter(Converter):

	def __init__(self):
		super(TrackMasterConverter, self).__init__()

		self._base_map = {
			"time=": "time",
			"latitude=": "latitude",
			"longitude=": "longitude",
			"speed=": "velocity kmh",
			"bearing=": "heading",
			"altitude=": "height"
		}

		self._user_map = {
			"lateral_accel=": ("LatAcc", "m/s2"),
			"accel=": ("LongAcc", "m/s2")
		}

		self._value_map.update({
			"time": self._datetime_to_secs,
		})

	@staticmethod
	def _datetime_to_secs(value):
		full = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
		# Get the difference the full time and start of the corresponding day.
		delta = full - full.combine(full.date(), time(0, tzinfo=full.tzinfo))
		return Decimal("%d.%d" % (delta.seconds, delta.microseconds))


def read_csv(input):
	reader = csv.reader(csv_input, delimiter=",", quotechar='"')

	# Filter out empty rows and strip all space around data items.
	rows = [[x.strip() for x in row] for row in reader if len(row) > 0]

	# The header row is the first row with the maximal number of items.
	head_row = max(rows, key=len)

	# Split the data at the header into info rows and data rows.
	# Eliminate potentially duplicate header rows from the data rows.
	head_index = rows.index(head_row)
	info_rows = rows[0:head_index]
	data_rows = [row for row in rows[head_index:] if row != head_row]

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


def interpolate_vbo(vbo_data, resolution):
	def _interpolate(row_a, row_b, offset, fraction):
		return [
			# a + f*(b-a)
			fraction.fma(val_b - val_a, val_a)
			if i != time_index else val_a + offset
			for i, (val_a, val_b) in enumerate(zip(row_a, row_b))
		]

	time_index = vbo_data.header().index("time")

	new_rows = []
	last_row = None
	for row in vbo_data.rows():
		next_row = [Decimal(value) for value in row]
		if last_row is not None:
			time_diff = next_row[time_index] - last_row[time_index]
			for step in range(1, ceil(time_diff / resolution)):
				offset = step * resolution
				fraction = offset / time_diff
				new_rows.append(
					_interpolate(last_row, next_row, offset, fraction)
				)

		new_rows.append(row)
		last_row = next_row

	return DataFrame(
		head=vbo_data.header(), data=new_rows,
		info=vbo_data.comments(), units=vbo_data.units()
	)


def format_vbo(vbo_data):
	def _seconds_to_hms(secs):
		int_secs = int(secs)
		# Get relative time in seconds (without the fractional part).
		rel_time = (datetime.min + timedelta(seconds=int_secs)).time()
		# Round the fractional part to milliseconds.
		int_msecs = int((secs - int_secs).quantize(
			# Always round halves up.
			Decimal("0.01"), rounding=ROUND_HALF_UP
		).shift(2))
		return "%s.%02d" % (rel_time.strftime("%H%M%S"), int_msecs)

	vbo_formatters = {
		"satellites": lambda v: "%03d" % v,
		"time": _seconds_to_hms,
		"latitude": lambda v: "%+012.5f" % v,
		"longitude": lambda v: "%+012.5f" % v,
		"velocity kmh": lambda v: "%07.3f" % v,
		"heading": lambda v: "%06.2f" % v,
		"height": lambda v: "%+09.2f" % v,
		"LatAcc": lambda v: "%+06.3f" % v,
		"LongAcc": lambda v: "%+06.3f" % v,
	}

	# Make sure we have all the formatters we need.
	formatters = [vbo_formatters.get(name) for name in vbo_data.header()]
	if None in formatters:
		raise Exception(
			"no formatter for %s" % vbo_data.header()[formatters.index(None)]
		)

	# Format all values in all rows.
	new_rows = [
		[fmt(val) for (val, fmt) in zip(vbo_row, formatters)]
		for vbo_row in vbo_data.rows()
	]

	return DataFrame(
		head=vbo_data.header(), data=new_rows,
		info=vbo_data.comments(), units=vbo_data.units()
	)


def write_vbo(vbo_data, vbo_output):
	# Required base types that make a log useful.
	base_types = OrderedDict((
		("satellites", "sats"), ("time", "time"),
		("latitude", "lat"), ("longitude", "long"),
	))

	# Optional base types that may be provided by a data logger.
	opt_base_types = OrderedDict((
		("velocity kmh", "velocity"), ("heading", "heading"),
		("height", "height"), ("vertical velocity m/s", "vert-vel"),
		("vertical velocity kmh", "vert-vel"),
		("yaw rate deg/s", "yaw-calc")
	))

	# Add only optional base types provided by the converter.
	base_types.update((
		(k, v) for (k, v) in opt_base_types.items()
			if k in vbo_data.header()
	))

	# Curry print() with the output file and MS-DOS line suffix.
	output = partial(print, file=vbo_output, end="\r\n")
	output(datetime.now().strftime("File created on %d/%m/%Y at %I:%M:%S %p"))

	output("\r\n[header]")
	for base_type in base_types.keys():
		output(base_type)

	# Collect the user-defined channel types and add them to the header.
	user_types = [x for x in vbo_data.header() if x not in base_types]
	for user_type in user_types:
		output(user_type)

	# Add units for user-defined channel types.
	if len(user_types) > 0:
		output("\r\n[channel units]")
		for user_type in user_types:
			output(vbo_data.units().get(user_type))

	# Output comments into the comments section. Single value rows become
	# comment lines. For multi-value rows, the first item becomes a label, and
	# the remaining items, if non-empty, are output separated by a semicolon.
	output("\r\n[comments]")
	for row in vbo_data.comments():
		if len(row) == 1:
			output(row[0])
		elif len(row) > 1 and len("".join(row[1:])) > 0:
			output("%s : %s" % (row[0].split(":")[0], ";".join(row[1:])))

	# Output column names for base types. For user types, the column names
	# are the same as the type names.
	output("\r\n[column names]")
	output(" ".join(list(base_types.values()) + user_types))

	# Determine the output order of VBO data types and get a map of
	# indices for getting the appropriate column from a VBO data row.
	out_order = list(base_types.keys()) + user_types
	indices = dict([(n, i) for (i, n) in enumerate(vbo_data.header())])

	output("\r\n[data]")
	for row in vbo_data.rows():
		out_row = [row[indices.get(name)] for name in out_order]
		output(" ".join(out_row))


### Read the CSV input, find a suitable converter,
### convert the CSV data to VBO data, and write the VBO output.

with sys.stdin as csv_input:
	csv_data = read_csv(csv_input)

converter = find_converter(csv_data)
if converter is None:
	print ("error: unable to recognize input format", file=sys.stderr)
	sys.exit(-1)

vbo_data = interpolate_vbo(converter.convert(csv_data), Decimal("0.10"))

with sys.stdout as vbo_output:
	write_vbo(format_vbo(vbo_data), vbo_output)
